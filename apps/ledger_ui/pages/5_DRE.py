import os
import sys
from pathlib import Path

# Adiciona o diretório raiz do projeto ao sys.path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = Path(script_dir).parent.parent.parent.parent  # raiz do projeto
sys.path.insert(0, str(project_root))

import streamlit as st
import pandas as pd
from pyaccount.builders.financial_statements import IncomeStatementBuilder
from pyaccount.core.account_mapper import AccountMapper

st.title("📈 DRE - Demonstração do Resultado do Exercício")

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

# Opções de agrupamento
st.sidebar.header("📊 Agrupamento")
agrupamento_opcoes = {
    "Sem Agrupamento": None,
    "Anual": "anual",
    "Mensal": "mensal",
    "Trimestral": "trimestral"
}
agrupamento_selecionado_nome = st.sidebar.selectbox(
    "Agrupamento por Período",
    options=list(agrupamento_opcoes.keys()),
    index=1,  # Muda padrão para "Anual"
    help="Selecione como agrupar os dados da DRE"
)
agrupamento_periodo = agrupamento_opcoes[agrupamento_selecionado_nome]

# Busca dados
df_pc = cli.buscar_plano_contas(empresa)

# Prepara movimentações com ou sem agrupamento por período
if agrupamento_periodo:
    # Busca lançamentos para calcular movimentações por período
    df_lanc = cli.buscar_lancamentos_periodo(empresa, inicio, fim)
    
    if df_lanc.empty:
        st.warning("Nenhum lançamento encontrado no período.")
        st.stop()
    
    # Converte data_lan para datetime se necessário
    if "data_lan" in df_lanc.columns:
        if not pd.api.types.is_datetime64_any_dtype(df_lanc["data_lan"]):
            df_lanc["data_lan"] = pd.to_datetime(df_lanc["data_lan"])
    
    # Calcula período baseado no tipo de agrupamento
    if agrupamento_periodo == "anual":
        # Formato: "2024", "2025", etc.
        df_lanc["periodo"] = df_lanc["data_lan"].dt.year.astype(str)
    elif agrupamento_periodo == "mensal":
        # Formato: "Jan/24", "Fev/24", etc.
        df_lanc["periodo"] = df_lanc["data_lan"].dt.strftime("%b/%y").str.title()
    elif agrupamento_periodo == "trimestral":
        # Formato: "1T/24", "2T/24", etc.
        df_lanc["trimestre"] = df_lanc["data_lan"].dt.quarter
        df_lanc["ano"] = df_lanc["data_lan"].dt.strftime("%y")
        df_lanc["periodo"] = df_lanc["trimestre"].astype(str) + "T/" + df_lanc["ano"]
        df_lanc = df_lanc.drop(columns=["trimestre", "ano"], errors="ignore")
    
    # Prepara dados para cálculo de movimentações por conta e período
    movimentos_lista = []
    
    # Processa débitos
    df_debitos = df_lanc[
        (df_lanc["cdeb_lan"].astype(str).str.strip() != "0") &
        (df_lanc["cdeb_lan"].notna())
    ].copy()
    
    if not df_debitos.empty:
        df_debitos["conta"] = df_debitos["cdeb_lan"].astype(str).str.strip()
        for _, row in df_debitos.iterrows():
            movimentos_lista.append({
                "conta": row["conta"],
                "periodo": row["periodo"],
                "movimento": float(row.get("vlor_lan", 0.0))
            })
    
    # Processa créditos
    df_creditos = df_lanc[
        (df_lanc["ccre_lan"].astype(str).str.strip() != "0") &
        (df_lanc["ccre_lan"].notna())
    ].copy()
    
    if not df_creditos.empty:
        df_creditos["conta"] = df_creditos["ccre_lan"].astype(str).str.strip()
        for _, row in df_creditos.iterrows():
            movimentos_lista.append({
                "conta": row["conta"],
                "periodo": row["periodo"],
                "movimento": -float(row.get("vlor_lan", 0.0))  # Negativo para créditos
            })
    
    # Agrupa por conta e período
    if movimentos_lista:
        df_mv_temp = pd.DataFrame(movimentos_lista)
        df_mv = df_mv_temp.groupby(["conta", "periodo"])["movimento"].sum().reset_index()
    else:
        df_mv = pd.DataFrame(columns=["conta", "periodo", "movimento"])
else:
    # Sem agrupamento - usa movimentações agregadas
    df_mv = cli.buscar_movimentacoes_periodo(empresa, inicio, fim)

# Gera DRE
dre = IncomeStatementBuilder(df_mv, df_pc, mapper, agrupamento_periodo=agrupamento_periodo).gerar()
st.dataframe(dre, width='stretch')

