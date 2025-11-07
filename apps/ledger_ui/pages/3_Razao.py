import os
import sys
from pathlib import Path

# Adiciona o diretório raiz do projeto ao sys.path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = Path(script_dir).parent.parent.parent.parent  # raiz do projeto
sys.path.insert(0, str(project_root))

import streamlit as st
import pandas as pd
import datetime
from pyaccount.core.account_mapper import AccountMapper

st.title("📖 Razão")

# Verifica se o cliente está conectado
if "_client" not in st.session_state:
    st.error("Por favor, conecte-se ao banco de dados na página principal.")
    st.stop()

cli = st.session_state["_client"]
empresa = st.session_state.get("empresa", 1)
inicio = st.session_state.get("inicio")
fim = st.session_state.get("fim")

if inicio is None or fim is None:
    st.error("Por favor, configure o período na página principal.")
    st.stop()

# Obtém classificação do modelo selecionado no app principal
classificacao_customizada = st.session_state.get("classificacao_customizada")
mapper = AccountMapper(classificacao_customizada=classificacao_customizada)

# Busca plano de contas para seleção
df_pc = cli.buscar_plano_contas(empresa)
if df_pc.empty:
    st.warning("Nenhuma conta encontrada.")
    st.stop()

# Verifica se colunas necessárias existem
colunas_necessarias = ["CLAS_CTA", "TIPO_CTA"]
colunas_faltantes = [c for c in colunas_necessarias if c not in df_pc.columns]
if colunas_faltantes:
    st.error(f"Colunas necessárias não encontradas no plano de contas: {', '.join(colunas_faltantes)}")
    st.stop()

# Garante que BC_GROUP existe - gera automaticamente se não existir ou estiver vazio
if "BC_GROUP" not in df_pc.columns:
    df_pc["BC_GROUP"] = None

# Preenche BC_GROUP vazio usando AccountMapper
mask_sem_bc_group = df_pc["BC_GROUP"].isna() | (df_pc["BC_GROUP"].astype(str).str.strip() == "")
if mask_sem_bc_group.any():
    df_pc.loc[mask_sem_bc_group, "BC_GROUP"] = df_pc.loc[mask_sem_bc_group].apply(
        lambda row: mapper.classificar_beancount(
            str(row.get("CLAS_CTA", "") or ""),
            str(row.get("TIPO_CTA", ""))
        ),
        axis=1
    )

df_pc["BC_GROUP"] = df_pc["BC_GROUP"].fillna("Unknown").astype(str)

# Função auxiliar para extrair níveis hierárquicos do BC_GROUP
def _extrair_niveis_bc_group(df_pc):
    """
    Extrai e organiza níveis hierárquicos do BC_GROUP.
    
    Returns:
        dict: Dicionário com estrutura {nivel: set de valores únicos}
    """
    niveis = {}
    for bc_group in df_pc["BC_GROUP"].dropna().unique():
        partes = str(bc_group).split(":")
        for i, parte in enumerate(partes):
            nivel = i + 1
            if nivel not in niveis:
                niveis[nivel] = set()
            niveis[nivel].add(parte.strip())
    return niveis

# Função para filtrar contas por caminho hierárquico
def _filtrar_contas_por_nivel(df_pc, caminho_hierarquico):
    """
    Filtra contas que começam com o caminho hierárquico especificado.
    
    Args:
        df_pc: DataFrame com plano de contas
        caminho_hierarquico: Lista com caminho (ex: ["Assets", "Ativo-Circulante"])
    
    Returns:
        DataFrame filtrado
    """
    if not caminho_hierarquico:
        return df_pc
    
    caminho_str = ":".join(caminho_hierarquico)
    mask = df_pc["BC_GROUP"].astype(str).str.startswith(caminho_str)
    return df_pc[mask].copy()

# Função para obter próximo nível de hierarquia
def _obter_proximo_nivel(df_pc, caminho_atual):
    """
    Obtém opções disponíveis para o próximo nível hierárquico.
    
    Args:
        df_pc: DataFrame com plano de contas
        caminho_atual: Lista com caminho atual (ex: ["Assets"])
    
    Returns:
        Lista de opções para o próximo nível
    """
    df_filtrado = _filtrar_contas_por_nivel(df_pc, caminho_atual)
    if df_filtrado.empty:
        return []
    
    proximo_nivel = len(caminho_atual) + 1
    opcoes = set()
    
    for bc_group in df_filtrado["BC_GROUP"].dropna().unique():
        partes = str(bc_group).split(":")
        if len(partes) >= proximo_nivel:
            opcoes.add(partes[proximo_nivel - 1].strip())
    
    return sorted(list(opcoes))

# Inicializa estado de navegação hierárquica
if "razao_caminho_hierarquico" not in st.session_state:
    st.session_state["razao_caminho_hierarquico"] = []

# Navegação hierárquica
st.header("🔍 Seleção Hierárquica de Conta")

# Nível 1: Grupos principais
niveis = _extrair_niveis_bc_group(df_pc)
if 1 not in niveis:
    st.error("Nenhum nível hierárquico encontrado no BC_GROUP.")
    st.stop()

grupos_principais = sorted(list(niveis[1]))

# Seleção do nível 1
if len(st.session_state["razao_caminho_hierarquico"]) == 0:
    grupo_selecionado = st.selectbox(
        "Nível 1 - Grupo Principal",
        options=[""] + grupos_principais,
        index=0,
        key="razao_nivel_1"
    )
    if grupo_selecionado:
        st.session_state["razao_caminho_hierarquico"] = [grupo_selecionado]
    else:
        st.info("👆 Selecione um grupo principal para começar.")
        st.stop()
else:
    # Mostra caminho atual
    caminho_display = " > ".join(st.session_state["razao_caminho_hierarquico"])
    st.info(f"📂 Caminho atual: **{caminho_display}**")
    
    # Botão para voltar
    if st.button("⬅️ Voltar ao início"):
        st.session_state["razao_caminho_hierarquico"] = []
        st.rerun()

# Navegação pelos níveis seguintes
caminho_atual = st.session_state["razao_caminho_hierarquico"]
df_filtrado = _filtrar_contas_por_nivel(df_pc, caminho_atual)

# Verifica se chegou em contas analíticas
contas_analiticas = df_filtrado[df_filtrado["TIPO_CTA"] == "A"]
proximo_nivel_opcoes = _obter_proximo_nivel(df_pc, caminho_atual)
tem_subniveis = len(proximo_nivel_opcoes) > 0

codigo_conta = None

# Mostra informações sobre contas disponíveis
st.caption(f"📊 {len(df_filtrado)} conta(s) encontrada(s), {len(contas_analiticas)} analítica(s)")

# Se não há mais subníveis OU há contas analíticas disponíveis, mostra seleção de contas
if not tem_subniveis:
    # Não há mais subníveis - mostra contas analíticas
    if contas_analiticas.empty:
        st.warning("Nenhuma conta analítica encontrada neste nível.")
        st.stop()
    
    contas_analiticas["conta_display"] = (
        contas_analiticas["CODI_CTA"].astype(str) + " - " + 
        contas_analiticas["NOME_CTA"].astype(str)
    )
    contas_lista = [""] + contas_analiticas["conta_display"].tolist()
    
    conta_selecionada = st.selectbox(
        "Conta Analítica",
        options=contas_lista,
        index=0,
        key="razao_conta_analitica"
    )
    
    if not conta_selecionada:
        st.info("👆 Selecione uma conta analítica para visualizar o razão.")
        st.stop()
    
    codigo_conta = conta_selecionada.split(" - ")[0]
elif not contas_analiticas.empty:
    # Há subníveis MAS também há contas analíticas - permite escolher entre continuar navegação ou selecionar conta
    st.markdown("---")
    st.subheader("Opções disponíveis")
    
    # Opção 1: Continuar navegação
    with st.expander("🔽 Continuar navegação hierárquica", expanded=True):
        nivel_num = len(caminho_atual) + 1
        nivel_selecionado = st.selectbox(
            f"Nível {nivel_num}",
            options=[""] + proximo_nivel_opcoes,
            index=0,
            key=f"razao_nivel_{nivel_num}"
        )
        
        if nivel_selecionado:
            st.session_state["razao_caminho_hierarquico"].append(nivel_selecionado)
            st.rerun()
    
    # Opção 2: Selecionar conta analítica diretamente
    with st.expander("📋 Selecionar conta analítica"):
        contas_analiticas["conta_display"] = (
            contas_analiticas["CODI_CTA"].astype(str) + " - " + 
            contas_analiticas["NOME_CTA"].astype(str)
        )
        contas_lista = [""] + contas_analiticas["conta_display"].tolist()
        
        conta_selecionada = st.selectbox(
            "Conta Analítica",
            options=contas_lista,
            index=0,
            key="razao_conta_analitica_direta"
        )
        
        if conta_selecionada:
            codigo_conta = conta_selecionada.split(" - ")[0]
    
    if codigo_conta is None:
        st.info("👆 Escolha uma opção acima para continuar.")
        st.stop()
else:
    # Ainda há subníveis e não há contas analíticas - continua navegação
    nivel_num = len(caminho_atual) + 1
    nivel_selecionado = st.selectbox(
        f"Nível {nivel_num}",
        options=[""] + proximo_nivel_opcoes,
        index=0,
        key=f"razao_nivel_{nivel_num}"
    )
    
    if nivel_selecionado:
        st.session_state["razao_caminho_hierarquico"].append(nivel_selecionado)
        st.rerun()
    else:
        st.info(f"👆 Selecione um subnível para continuar a navegação.")
        st.stop()

# Se chegou aqui, tem código de conta selecionado
if codigo_conta is None:
    st.stop()

# Busca saldo anterior (até o dia anterior ao início do período)
saldo_anterior = cli.buscar_saldos(empresa, inicio - datetime.timedelta(days=1))
saldo_inicial = saldo_anterior[saldo_anterior["conta"] == codigo_conta]["saldo"].values
saldo_inicial_valor = saldo_inicial[0] if len(saldo_inicial) > 0 else 0.0

# Busca lançamentos do período
df_lancamentos = cli.buscar_lancamentos_periodo(empresa, inicio, fim)

# Filtra lançamentos da conta selecionada (débito ou crédito)
if not df_lancamentos.empty:
    # Filtra lançamentos onde a conta aparece como débito ou crédito
    mask_debito = (df_lancamentos["cdeb_lan"].astype(str).str.strip() == codigo_conta)
    mask_credito = (df_lancamentos["ccre_lan"].astype(str).str.strip() == codigo_conta)
    df_conta = df_lancamentos[mask_debito | mask_credito].copy()
    
    if df_conta.empty:
        st.info(f"Nenhuma movimentação encontrada para a conta {codigo_conta} no período selecionado.")
        # Mostra apenas saldo inicial
        df_razao = pd.DataFrame([{
            "Data": inicio - datetime.timedelta(days=1),
            "Histórico": "SALDO ANTERIOR",
            "Documento": "",
            "Débito": 0.0,
            "Crédito": 0.0,
            "Saldo": saldo_inicial_valor
        }])
        st.dataframe(df_razao, width='stretch', hide_index=True)
        st.stop()
    
    # Prepara dados do razão
    linhas_razao = []
    
    # Primeira linha: Saldo Anterior
    linhas_razao.append({
        "Data": inicio - datetime.timedelta(days=1),
        "Histórico": "SALDO ANTERIOR",
        "Documento": "",
        "Débito": 0.0,
        "Crédito": 0.0,
        "Saldo": saldo_inicial_valor
    })
    
    # Processa cada lançamento
    saldo_atual = saldo_inicial_valor
    df_conta = df_conta.sort_values("data_lan")
    
    for _, lanc in df_conta.iterrows():
        data_lan = lanc.get("data_lan", inicio)
        historico = str(lanc.get("chis_lan", "") or "")
        documento = str(lanc.get("ndoc_lan", "") or "")
        valor = float(lanc.get("vlor_lan", 0.0))
        
        # Verifica se é débito ou crédito
        is_debito = str(lanc.get("cdeb_lan", "")).strip() == codigo_conta
        is_credito = str(lanc.get("ccre_lan", "")).strip() == codigo_conta
        
        debito = valor if is_debito else 0.0
        credito = valor if is_credito else 0.0
        
        # Calcula novo saldo
        if is_debito:
            saldo_atual += debito
        elif is_credito:
            saldo_atual -= credito
        
        linhas_razao.append({
            "Data": data_lan,
            "Histórico": historico,
            "Documento": documento,
            "Débito": debito,
            "Crédito": credito,
            "Saldo": saldo_atual
        })
    
    # Cria DataFrame do razão
    df_razao = pd.DataFrame(linhas_razao)
    
    # Formata valores para exibição
    df_razao["Débito"] = df_razao["Débito"].apply(lambda x: f"{x:,.2f}" if x > 0 else "")
    df_razao["Crédito"] = df_razao["Crédito"].apply(lambda x: f"{x:,.2f}" if x > 0 else "")
    df_razao["Saldo"] = df_razao["Saldo"].apply(lambda x: f"{x:,.2f}")
    
    # Exibe razão
    st.dataframe(df_razao, width='stretch', hide_index=True)
    
    # Mostra resumo
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Saldo Anterior", f"{saldo_inicial_valor:,.2f}")
    with col2:
        total_debitos = df_conta[df_conta["cdeb_lan"].astype(str).str.strip() == codigo_conta]["vlor_lan"].sum()
        st.metric("Total Débitos", f"{total_debitos:,.2f}")
    with col3:
        total_creditos = df_conta[df_conta["ccre_lan"].astype(str).str.strip() == codigo_conta]["vlor_lan"].sum()
        st.metric("Total Créditos", f"{total_creditos:,.2f}")
    with col4:
        saldo_final = saldo_inicial_valor + total_debitos - total_creditos
        st.metric("Saldo Final", f"{saldo_final:,.2f}")
else:
    st.info("Nenhum lançamento encontrado no período selecionado.")

