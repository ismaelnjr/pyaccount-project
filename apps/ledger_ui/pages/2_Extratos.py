import os
import sys
from pathlib import Path

# Adiciona o diretório raiz do projeto ao sys.path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = Path(script_dir).parent.parent.parent.parent  # raiz do projeto
sys.path.insert(0, str(project_root))

import streamlit as st
from pyaccount.builders.financial_statements import PeriodMovementsBuilder
from pyaccount.core.account_mapper import AccountMapper

st.title("📋 Extratos")

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

# Busca lançamentos
df_lc = cli.buscar_lancamentos_periodo(empresa, inicio, fim)

# Gera extratos
extr = PeriodMovementsBuilder(df_lc, mapper).gerar()
st.dataframe(extr, width='stretch')

