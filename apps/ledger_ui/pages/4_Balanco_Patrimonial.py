import os
import sys
from pathlib import Path

# Adiciona o diretório raiz do projeto ao sys.path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = Path(script_dir).parent.parent.parent.parent  # raiz do projeto
sys.path.insert(0, str(project_root))

import streamlit as st
from pyaccount.builders.financial_statements import BalanceSheetBuilder
from pyaccount.core.account_mapper import AccountMapper

st.title("⚖️ Balanço Patrimonial")

# Verifica se o cliente está conectado
if "_client" not in st.session_state:
    st.error("Por favor, conecte-se ao banco de dados na página principal.")
    st.stop()

cli = st.session_state["_client"]
empresa = st.session_state.get("empresa", 1)
fim = st.session_state.get("fim")

if fim is None:
    st.error("Por favor, configure o período na página principal.")
    st.stop()

# Obtém classificação do modelo selecionado no app principal
classificacao_customizada = st.session_state.get("classificacao_customizada")
mapper = AccountMapper(classificacao_customizada=classificacao_customizada)

# Busca dados
df_pc = cli.buscar_plano_contas(empresa)
df_sf = cli.buscar_saldos(empresa, fim)

# Gera balanço patrimonial
bp = BalanceSheetBuilder(df_sf, df_pc, mapper).gerar()
st.dataframe(bp, width='stretch')

