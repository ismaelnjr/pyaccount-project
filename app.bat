@echo off
rem Remove o arquivo logs/queries.log se existir
if exist logs\queries.log del logs\queries.log
rem Executa o streamlit
python -m streamlit run apps/ledger_ui/app.py