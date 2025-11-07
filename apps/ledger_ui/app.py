import os
import sys
from pathlib import Path

# Adiciona o diretório raiz do projeto ao sys.path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = Path(script_dir).parent.parent.parent  # raiz do projeto
sys.path.insert(0, str(project_root))

import streamlit as st
from datetime import date
from pyaccount.data.clients.sqlite import SQLiteClient
from pyaccount.core.account_classifier import TipoPlanoContas, obter_classificacao_do_modelo

st.set_page_config(page_title="Navegação Contábil — SQLite", layout="wide")

st.title("📘 Navegação Contábil — SQLite")
st.markdown("---")

# Selecionar/conectar DB
st.sidebar.header("🔌 Conexão")
db_file = st.sidebar.text_input("Arquivo .db", value=str(Path.cwd() / "contas.db"))

# Opções de logging
st.sidebar.header("📝 Log de Consultas")
enable_query_log = st.sidebar.checkbox("Habilitar log de queries SQL", value=True)
query_log_file = st.sidebar.text_input("Arquivo de log", value="logs/queries.log", disabled=not enable_query_log)

if st.sidebar.button("Conectar"):
    try:
        st.session_state["_client"] = SQLiteClient(
            db_file,
            enable_query_log=enable_query_log,
            query_log_file=query_log_file if enable_query_log else "logs/queries.log"
        )
        st.sidebar.success("✅ Conectado com sucesso!")
        if enable_query_log:
            st.sidebar.info(f"📝 Log de queries habilitado: {query_log_file}")
    except Exception as e:
        st.sidebar.error(f"❌ Erro ao conectar: {e}")

if "_client" not in st.session_state:
    st.info("👆 Informe o caminho do arquivo .db e clique em **Conectar** para começar.")
    st.stop()

# Parâmetros comuns
st.sidebar.header("⚙️ Parâmetros")

# Busca empresas disponíveis
cli = st.session_state["_client"]
try:
    df_empresas = cli.listar_empresas()
    if df_empresas.empty:
        st.sidebar.warning("⚠️ Nenhuma empresa cadastrada. Use o script de importação com --nome-empresa para cadastrar empresas.")
        empresa = st.sidebar.number_input("Empresa", min_value=1, value=1, step=1)
    else:
        # Cria opções no formato "CODI_EMP - NOME"
        opcoes_empresas = [f"{row['CODI_EMP']} - {row['NOME']}" for _, row in df_empresas.iterrows()]
        empresa_selecionada = st.sidebar.selectbox(
            "Empresa",
            options=opcoes_empresas,
            index=0,
            help="Selecione a empresa para visualizar os relatórios"
        )
        # Extrai CODI_EMP da opção selecionada
        empresa = int(empresa_selecionada.split(" - ")[0])
except Exception as e:
    st.sidebar.error(f"Erro ao buscar empresas: {e}")
    empresa = st.sidebar.number_input("Empresa", min_value=1, value=1, step=1)
inicio = st.sidebar.date_input("Início", value=date(date.today().year, 1, 1))
fim = st.sidebar.date_input("Fim", value=date.today())

# Modelo de plano de contas
st.sidebar.header("📊 Classificação")
modelo_opcoes = {
    "Padrão Brasileiro": TipoPlanoContas.PADRAO,
    "Simplificado": TipoPlanoContas.SIMPLIFICADO,
    "IFRS": TipoPlanoContas.IFRS
}
modelo_selecionado_nome = st.sidebar.selectbox(
    "Modelo de Plano de Contas",
    options=list(modelo_opcoes.keys()),
    index=0,
    help="Selecione o modelo de classificação contábil a ser usado"
)
modelo_selecionado = modelo_opcoes[modelo_selecionado_nome]

# Salva no session_state para as páginas acessarem
st.session_state["empresa"] = empresa
st.session_state["inicio"] = inicio
st.session_state["fim"] = fim
st.session_state["modelo_plano_contas"] = modelo_selecionado
st.session_state["classificacao_customizada"] = obter_classificacao_do_modelo(modelo_selecionado)

st.sidebar.markdown("---")
st.sidebar.info("💡 Use o menu lateral para navegar entre as páginas de relatórios.")

# Página inicial
st.markdown("""
## Bem-vindo ao Navegação Contábil

Este aplicativo permite visualizar e analisar dados contábeis armazenados em SQLite.

### 📋 Páginas Disponíveis:

- **Balancete** - Visão consolidada de saldos e movimentações
- **Extratos** - Detalhamento de lançamentos do período
- **Razão** - Razão analítico com saldo acumulado
- **Balanço Patrimonial** - Estrutura patrimonial
- **DRE** - Demonstração do Resultado do Exercício

### 🚀 Como usar:

1. Conecte-se ao banco de dados usando o formulário na barra lateral
2. Configure os parâmetros (empresa e período)
3. Navegue pelas páginas usando o menu lateral
""")
