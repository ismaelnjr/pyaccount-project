import os
import sys
import argparse
import json
import configparser
from pathlib import Path

# Adiciona o diretório raiz do projeto ao sys.path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)  # raiz do projeto
sys.path.insert(0, project_root)

from pyaccount.data.ingest.sqlite_elt import init_db, import_plano_contas, import_saldos_iniciais, import_lancamentos, import_empresas
from pyaccount.core.account_classifier import TipoPlanoContas, obter_classificacao_do_modelo

parser = argparse.ArgumentParser(description="Importa dados contábeis de CSV para SQLite")
parser.add_argument("--db", required=True, help="Caminho do banco de dados SQLite")
parser.add_argument("--empresas", help="Caminho do arquivo CSV de empresas (formato: CODI_EMP;NOME sem cabeçalho)")
parser.add_argument("--plano", help="Caminho do arquivo CSV do plano de contas")
parser.add_argument("--saldos", help="Caminho do arquivo CSV de saldos iniciais")
parser.add_argument("--lanc", help="Caminho do arquivo CSV de lançamentos")
parser.add_argument("--empresa", type=int, help="Código da empresa (necessário se saldos não contém codi_emp)")
parser.add_argument("--nome-empresa", help="Nome da empresa (opcional). Se fornecido, cria/atualiza empresa na tabela empresas")
parser.add_argument("--modelo", choices=["padrao", "simplificado", "ifrs", "customizado"], 
                    help="Modelo de classificação de contas (padrão: padrao). Use 'customizado' para carregar do config.ini")
parser.add_argument("--classificacao", help="Caminho para arquivo JSON com classificação customizada (ex: {\"1\": \"Assets:Custom\"})")
parser.add_argument("--config", help="Caminho para config.ini (usado quando modelo=customizado)")
args = parser.parse_args()

# Carrega classificação customizada
classificacao_customizada = None
if args.classificacao:
    # Carrega de arquivo JSON
    with open(args.classificacao, 'r', encoding='utf-8') as f:
        classificacao_customizada = json.load(f)
elif args.modelo == "customizado":
    # Carrega do config.ini quando modelo=customizado
    config_path = Path(args.config) if args.config else Path(project_root) / "config.ini"
    if config_path.exists():
        try:
            cfg = configparser.ConfigParser()
            cfg.read(config_path)
            
            if cfg.has_section("classification"):
                # Extrai clas_base (opcional)
                clas_base_str = cfg.get("classification", "clas_base", fallback="").strip()
                clas_base = None
                if clas_base_str:
                    clas_base_map = {
                        "CLASSIFICACAO_PADRAO_BR": TipoPlanoContas.PADRAO,
                        "padrao": TipoPlanoContas.PADRAO,
                        "CLASSIFICACAO_SIMPLIFICADO": TipoPlanoContas.SIMPLIFICADO,
                        "simplificado": TipoPlanoContas.SIMPLIFICADO,
                        "CLASSIFICACAO_IFRS": TipoPlanoContas.IFRS,
                        "ifrs": TipoPlanoContas.IFRS,
                    }
                    clas_base = clas_base_map.get(clas_base_str)
                
                # Extrai todas as entradas clas_* (exceto clas_base)
                classificacao_dict = {}
                for chave, valor in cfg.items("classification"):
                    if chave.startswith("clas_") and chave != "clas_base":
                        prefixo = chave.replace("clas_", "")
                        classificacao_dict[prefixo] = valor.strip()
                
                # Valida: se não houver clas_base e nenhuma entrada clas_*, gera erro
                if not clas_base and not classificacao_dict:
                    print(f"ERRO: modelo=customizado requer pelo menos clas_base ou entradas clas_* na seção [classification]", file=sys.stderr)
                    sys.exit(1)
                
                # Obtém classificação completa usando clas_base e customizações
                classificacao_customizada = obter_classificacao_do_modelo(
                    modelo=None,
                    customizacoes=classificacao_dict,
                    clas_base=clas_base,
                    usar_apenas_customizacoes=True
                )
                print(f"✓ Classificação customizada carregada do config.ini: {len(classificacao_customizada)} entradas")
        except Exception as e:
            print(f"ERRO ao carregar classificação customizada do config.ini: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"ERRO: config.ini não encontrado em {config_path}", file=sys.stderr)
        sys.exit(1)

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
