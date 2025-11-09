from pathlib import Path
import sqlite3
import pandas as pd
from typing import Optional, Dict

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"

def _ler_csv_com_encoding(csv_path: str, sep: str = ";", **kwargs):
    """Tenta ler CSV com diferentes encodings."""
    encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252", "iso-8859-1"]
    for encoding in encodings:
        try:
            # Usa engine='python' e on_bad_lines='skip' para lidar com linhas inconsistentes
            return pd.read_csv(
                csv_path, 
                sep=sep, 
                encoding=encoding, 
                engine='python',
                on_bad_lines='skip',
                **kwargs
            )
        except (UnicodeDecodeError, pd.errors.EmptyDataError):
            continue
    raise ValueError(f"Não foi possível ler o arquivo {csv_path} com nenhum encoding suportado")

def criar_ou_atualizar_empresa(db_path: str, codi_emp: int, nome: str):
    """
    Cria ou atualiza uma empresa na tabela empresas.
    
    Args:
        db_path: Caminho do banco de dados SQLite
        codi_emp: Código da empresa
        nome: Nome da empresa
    """
    with sqlite3.connect(db_path) as con:
        # Usa INSERT OR REPLACE para inserir ou atualizar
        con.execute(
            "INSERT OR REPLACE INTO empresas (CODI_EMP, NOME) VALUES (?, ?)",
            (codi_emp, nome)
        )
        con.commit()

def import_empresas(db_path: str, csv_path: str, sep: str=";"):
    """
    Importa empresas do CSV.
    
    Args:
        db_path: Caminho do banco de dados SQLite
        csv_path: Caminho do arquivo CSV
        sep: Separador do CSV (padrão: ";")
    """
    # CSV sem cabeçalho: CODI_EMP;NOME (2 colunas)
    df = _ler_csv_com_encoding(csv_path, sep=sep, dtype=str, header=None)
    
    if df.empty:
        print(f"[AVISO] Arquivo {csv_path} está vazio.")
        return
    
    if len(df.columns) != 2:
        raise ValueError(f"Formato de CSV não suportado. Esperado 2 colunas (CODI_EMP;NOME), encontrado {len(df.columns)}")
    
    df.columns = ["CODI_EMP", "NOME"]
    
    # Remove linhas vazias ou com valores nulos
    df = df.dropna(subset=["CODI_EMP", "NOME"])
    df = df[(df["CODI_EMP"].astype(str).str.strip() != "") & (df["NOME"].astype(str).str.strip() != "")]
    
    if df.empty:
        print(f"[AVISO] Nenhuma empresa válida encontrada em {csv_path}.")
        return
    
    # Converte CODI_EMP para inteiro
    try:
        df["CODI_EMP"] = df["CODI_EMP"].astype(int)
    except ValueError as e:
        raise ValueError(f"Erro ao converter CODI_EMP para inteiro: {e}")
    
    # Importa cada empresa
    for _, row in df.iterrows():
        codi_emp = int(row["CODI_EMP"])
        nome = str(row["NOME"]).strip()
        criar_ou_atualizar_empresa(db_path, codi_emp, nome)
    
    print(f"[OK] Importadas {len(df)} empresa(s) de {csv_path}")

def init_db(db_path: str):
    sql = Path(SCHEMA_PATH).read_text(encoding="utf-8")
    with sqlite3.connect(db_path) as con:
        con.executescript(sql)
        # Adiciona coluna bc_group se não existir (para compatibilidade com bancos antigos)
        try:
            con.execute("ALTER TABLE plano_contas ADD COLUMN bc_group TEXT")
        except sqlite3.OperationalError:
            pass  # Coluna já existe
        
        # Migração da tabela empresas: se existir com estrutura antiga (id, nome), migra para nova (CODI_EMP, NOME)
        try:
            # Verifica se tabela empresas existe
            cursor = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='empresas'")
            if cursor.fetchone():
                # Verifica estrutura atual
                cursor = con.execute("PRAGMA table_info(empresas)")
                colunas = [row[1] for row in cursor.fetchall()]  # row[1] é o nome da coluna
                
                if "id" in colunas and "CODI_EMP" not in colunas:
                    # Estrutura antiga: precisa migrar
                    # Cria tabela temporária com nova estrutura
                    con.execute("""
                        CREATE TABLE IF NOT EXISTS empresas_nova (
                            CODI_EMP INTEGER PRIMARY KEY,
                            NOME TEXT NOT NULL
                        )
                    """)
                    # Copia dados: se id for numérico, usa como CODI_EMP; senão, tenta inferir
                    try:
                        con.execute("""
                            INSERT INTO empresas_nova (CODI_EMP, NOME)
                            SELECT CAST(id AS INTEGER) AS CODI_EMP, nome AS NOME
                            FROM empresas
                            WHERE nome IS NOT NULL
                        """)
                    except:
                        # Se falhar, tenta usar id diretamente
                        con.execute("""
                            INSERT INTO empresas_nova (CODI_EMP, NOME)
                            SELECT id AS CODI_EMP, nome AS NOME
                            FROM empresas
                            WHERE nome IS NOT NULL
                        """)
                    # Remove tabela antiga e renomeia nova
                    con.execute("DROP TABLE empresas")
                    con.execute("ALTER TABLE empresas_nova RENAME TO empresas")
                    con.commit()
                elif "CODI_EMP" not in colunas:
                    # Tabela existe mas sem CODI_EMP: recria
                    con.execute("DROP TABLE empresas")
                    con.execute("""
                        CREATE TABLE empresas (
                            CODI_EMP INTEGER PRIMARY KEY,
                            NOME TEXT NOT NULL
                        )
                    """)
                    con.commit()
        except sqlite3.OperationalError:
            pass  # Tabela não existe ou erro na migração (será criada pelo schema)
        
        # Garante que empresa 267 "OC ALIMENTOS" existe
        con.execute("INSERT OR IGNORE INTO empresas (CODI_EMP, NOME) VALUES (267, 'OC ALIMENTOS')")
        con.commit()

def import_plano_contas(
    db_path: str, 
    csv_path: str, 
    sep: str=";",
    modelo: Optional[str] = None,
    classificacao_customizada: Optional[Dict[str, str]] = None,
    nome_empresa: Optional[str] = None
):
    """
    Importa plano de contas do CSV e calcula classificação BC_GROUP.
    
    Args:
        db_path: Caminho do banco de dados SQLite
        csv_path: Caminho do arquivo CSV
        sep: Separador do CSV (padrão: ";")
        modelo: Modelo de classificação ("padrao", "simplificado", "ifrs") ou None para padrão
        classificacao_customizada: Dicionário com mapeamento customizado CLAS_CTA -> BC_GROUP
        nome_empresa: Nome da empresa (opcional). Se fornecido, cria/atualiza empresa na tabela empresas
    """
    # CSV sem cabeçalho: codi_emp;codi_cta;nome_cta;clas_cta;tipo_cta;data_cta;situacao_cta (7 colunas)
    df = _ler_csv_com_encoding(csv_path, sep=sep, dtype=str, header=None)
    df.columns = ["codi_emp","codi_cta","nome_cta","clas_cta","tipo_cta","data_cta","situacao_cta"]
    
    # Se nome_empresa foi fornecido, cria/atualiza empresa
    if nome_empresa:
        # Obtém codi_emp do primeiro registro (assumindo que todos são da mesma empresa)
        codi_emp = int(df["codi_emp"].iloc[0])
        criar_ou_atualizar_empresa(db_path, codi_emp, nome_empresa)
    
    # Importa AccountMapper para classificar
    from pyaccount.core.account_mapper import AccountMapper
    from pyaccount.core.account_classifier import obter_classificacao_do_modelo, TipoPlanoContas
    
    # Converte string do modelo para enum ou trata customizado
    modelo_enum = None
    clas_base = None
    usar_apenas_customizacoes = False
    
    if modelo and modelo.lower() == "customizado":
        # Modelo customizado: usa apenas classificacao_customizada
        usar_apenas_customizacoes = True
        # clas_base pode ser passado via classificacao_customizada se necessário
        # Por enquanto, assumimos que classificacao_customizada já contém a classificação completa
        # ou que clas_base será None (dicionário vazio como base)
    elif modelo:
        modelo_map = {
            "padrao": TipoPlanoContas.PADRAO,
            "simplificado": TipoPlanoContas.SIMPLIFICADO,
            "ifrs": TipoPlanoContas.IFRS
        }
        modelo_enum = modelo_map.get(modelo.lower())
    
    # Obtém classificação
    classificacao = obter_classificacao_do_modelo(
        modelo_enum, 
        classificacao_customizada,
        clas_base=clas_base,
        usar_apenas_customizacoes=usar_apenas_customizacoes
    )
    mapper = AccountMapper(classificacao)
    
    # Calcula BC_GROUP para cada conta
    df["bc_group"] = df.apply(
        lambda row: mapper.classificar_beancount(
            str(row.get("clas_cta", "") or ""),
            str(row.get("tipo_cta", ""))
        ),
        axis=1
    )
    
    # Selecionar apenas as colunas necessárias (sem data_cta que não está no schema)
    cols = ["codi_emp","codi_cta","nome_cta","clas_cta","tipo_cta","situacao_cta","bc_group"]
    df = df[cols]
    df.to_sql("plano_contas", sqlite3.connect(db_path), if_exists="append", index=False)

def import_saldos_iniciais(db_path: str, csv_path: str, sep: str=";", codi_emp: int = None, nome_empresa: Optional[str] = None):
    """
    Importa saldos iniciais do CSV.
    
    Args:
        db_path: Caminho do banco de dados SQLite
        csv_path: Caminho do arquivo CSV
        sep: Separador do CSV (padrão: ";")
        codi_emp: Código da empresa (necessário se CSV não contém codi_emp)
        nome_empresa: Nome da empresa (opcional). Se fornecido, cria/atualiza empresa na tabela empresas
    """
    # CSV sem cabeçalho: conta;saldo;data_saldo (3 colunas) - falta codi_emp
    df = _ler_csv_com_encoding(csv_path, sep=sep, header=None)
    
    if len(df.columns) == 3:
        # Formato sem codi_emp: conta;saldo;data_saldo
        df.columns = ["conta", "saldo", "data_saldo"]
        if codi_emp is None:
            # Tentar inferir do banco de dados
            with sqlite3.connect(db_path) as con:
                result = con.execute("SELECT DISTINCT codi_emp FROM plano_contas LIMIT 1").fetchone()
                if result:
                    codi_emp = result[0]
                else:
                    result = con.execute("SELECT DISTINCT codi_emp FROM lancamentos LIMIT 1").fetchone()
                    if result:
                        codi_emp = result[0]
                    else:
                        raise ValueError("CSV de saldos não contém codi_emp e não foi possível inferir do banco. Forneça o parâmetro codi_emp ou use formato: codi_emp;conta;saldo;data_saldo")
        df["codi_emp"] = codi_emp
    elif len(df.columns) == 4:
        # Formato com codi_emp: codi_emp;conta;saldo;data_saldo
        df.columns = ["codi_emp", "conta", "saldo", "data_saldo"]
        # Se codi_emp não foi fornecido como parâmetro, usa do CSV
        if codi_emp is None:
            codi_emp = int(df["codi_emp"].iloc[0])
    else:
        raise ValueError(f"Formato de CSV não suportado. Esperado 3 ou 4 colunas, encontrado {len(df.columns)}")
    
    # Se nome_empresa foi fornecido, cria/atualiza empresa
    if nome_empresa:
        criar_ou_atualizar_empresa(db_path, codi_emp, nome_empresa)
    
    # Converter saldo de vírgula para ponto decimal
    df["saldo"] = df["saldo"].astype(str).str.replace(",", ".", regex=False)
    df["saldo"] = pd.to_numeric(df["saldo"], errors="coerce").fillna(0.0)
    
    # Converter data (formato YYYYMMDD)
    df["data_saldo"] = pd.to_datetime(df["data_saldo"].astype(str), format="%Y%m%d", errors="coerce").dt.date
    
    cols = ["codi_emp","conta","data_saldo","saldo"]
    df = df.reindex(columns=cols)
    df.to_sql("saldos_iniciais", sqlite3.connect(db_path), if_exists="append", index=False)

def import_lancamentos(db_path: str, csv_path: str, sep: str=";", nome_empresa: Optional[str] = None):
    """
    Importa lançamentos do CSV.
    
    Args:
        db_path: Caminho do banco de dados SQLite
        csv_path: Caminho do arquivo CSV
        sep: Separador do CSV (padrão: ";")
        nome_empresa: Nome da empresa (opcional). Se fornecido, cria/atualiza empresa na tabela empresas
    """
    # CSV sem cabeçalho: codi_emp;nume_lan;data_lan;codi_lote;tipo_lote;codi_his;chis_lan;ndoc_lan;codi_usu;natureza;conta;nome_cta;clas_cta;valor (14 colunas)
    df = _ler_csv_com_encoding(csv_path, sep=sep, header=None)
    
    if len(df.columns) == 14:
        # Formato com nome_cta e clas_cta extras
        df.columns = ["codi_emp","nume_lan","data_lan","codi_lote","tipo_lote",
                      "codi_his","chis_lan","ndoc_lan","codi_usu","natureza",
                      "conta","nome_cta","clas_cta","valor"]
    elif len(df.columns) == 12:
        # Formato sem nome_cta e clas_cta
        df.columns = ["codi_emp","nume_lan","data_lan","codi_lote","tipo_lote",
                      "codi_his","chis_lan","ndoc_lan","codi_usu","natureza",
                      "conta","valor"]
    else:
        raise ValueError(f"Formato de CSV não suportado. Esperado 12 ou 14 colunas, encontrado {len(df.columns)}")
    
    # Se nome_empresa foi fornecido, cria/atualiza empresa
    if nome_empresa:
        # Obtém codi_emp do primeiro registro (assumindo que todos são da mesma empresa)
        codi_emp = int(df["codi_emp"].iloc[0])
        criar_ou_atualizar_empresa(db_path, codi_emp, nome_empresa)
    
    # Normalizações
    df["data_lan"] = pd.to_datetime(df["data_lan"].astype(str), format="%Y%m%d", errors="coerce").dt.date
    # Converter valor de vírgula para ponto decimal
    df["valor"] = df["valor"].astype(str).str.replace(",", ".", regex=False)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce").abs()
    df = df.rename(columns={"natureza":"lado"})
    
    # Selecionar apenas as colunas necessárias
    cols = ["codi_emp","nume_lan","data_lan","codi_lote","tipo_lote",
            "codi_his","chis_lan","ndoc_lan","codi_usu","lado","conta","valor"]
    df = df.reindex(columns=cols)
    df.to_sql("lancamentos", sqlite3.connect(db_path), if_exists="append", index=False)
