import os
import sys
import argparse
import json

# Adiciona o diretório raiz do projeto ao sys.path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)  # raiz do projeto
sys.path.insert(0, project_root)

from pyaccount.data.ingest.sqlite_elt import init_db, import_plano_contas, import_saldos_iniciais, import_lancamentos, import_empresas

parser = argparse.ArgumentParser(description="Importa dados contábeis de CSV para SQLite")
parser.add_argument("--db", required=True, help="Caminho do banco de dados SQLite")
parser.add_argument("--empresas", help="Caminho do arquivo CSV de empresas (formato: CODI_EMP;NOME sem cabeçalho)")
parser.add_argument("--plano", help="Caminho do arquivo CSV do plano de contas")
parser.add_argument("--saldos", help="Caminho do arquivo CSV de saldos iniciais")
parser.add_argument("--lanc", help="Caminho do arquivo CSV de lançamentos")
parser.add_argument("--empresa", type=int, help="Código da empresa (necessário se saldos não contém codi_emp)")
parser.add_argument("--nome-empresa", help="Nome da empresa (opcional). Se fornecido, cria/atualiza empresa na tabela empresas")
parser.add_argument("--modelo", choices=["padrao", "simplificado", "ifrs"], 
                    help="Modelo de classificação de contas (padrão: padrao)")
parser.add_argument("--classificacao", help="Caminho para arquivo JSON com classificação customizada (ex: {\"1\": \"Assets:Custom\"})")
args = parser.parse_args()

# Carrega classificação customizada se fornecida
classificacao_customizada = None
if args.classificacao:
    with open(args.classificacao, 'r', encoding='utf-8') as f:
        classificacao_customizada = json.load(f)

init_db(args.db)

# Importa empresas primeiro (se fornecido)
if args.empresas:
    import_empresas(args.db, args.empresas)

# Depois importa outros dados
if args.plano:
    import_plano_contas(
        args.db, 
        args.plano, 
        modelo=args.modelo,
        classificacao_customizada=classificacao_customizada,
        nome_empresa=args.nome_empresa
    )
if args.saldos:
    import_saldos_iniciais(args.db, args.saldos, codi_emp=args.empresa, nome_empresa=args.nome_empresa)
if args.lanc:
    import_lancamentos(args.db, args.lanc, nome_empresa=args.nome_empresa)

print("OK")
