#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera arquivo de Saldos Iniciais (cache) até uma data de corte (D_corte),
para evitar varredura histórica a cada execução do pipeline principal.

Uso:
  python build_opening_balances.py \
      --dsn SQLANYWHERE17 --user dba --password sql \
      --empresa 437 --ate 2025-08-31 \
      --saida ./out

Saída:
  out/saldos_iniciais_<empresa>_<ate>.csv
  (colunas: conta, saldo, clas_cta, bc_account, empresa, data_corte)
"""
import argparse
import configparser
from pathlib import Path
from datetime import date
from typing import Optional, Dict
import sys

import pyodbc
import pandas as pd
from dateutil.parser import isoparse


class OpeningBalancesBuilder:
    """
    Constrói arquivos de saldos iniciais para Beancount a partir de banco de dados.
    
    Esta classe encapsula toda a lógica necessária para:
    - Conectar-se ao banco de dados
    - Buscar plano de contas
    - Buscar saldos até uma data específica
    - Mapear contas do sistema contábil para Beancount
    - Exportar resultados em CSV
    """
    
    def __init__(self, dsn: str, user: str, password: str, empresa: int, ate: date, saida: Path):
        """
        Inicializa o construtor de saldos iniciais.
        
        Args:
            dsn: Nome do DSN ODBC
            user: Usuário do banco de dados
            password: Senha do banco de dados
            empresa: Código da empresa
            ate: Data de corte (até quando calcular os saldos)
            saida: Diretório de saída dos arquivos
        """
        self.dsn = dsn
        self.user = user
        self.password = password
        self.empresa = empresa
        self.ate = ate
        self.saida = Path(saida)
        self.conn: Optional[pyodbc.Connection] = None
        self.df_pc: Optional[pd.DataFrame] = None
        self.df_saldos: Optional[pd.DataFrame] = None
        self.mapa_clas_to_bc: Dict[str, str] = {}
    
    def connect(self) -> None:
        """Estabelece conexão com o banco de dados."""
        if not all([self.dsn, self.user, self.password]):
            raise ValueError("DSN, user e password devem ser fornecidos.")
        
        try:
            self.conn = pyodbc.connect(f"DSN={self.dsn};UID={self.user};PWD={self.password}")
        except Exception as e:
            raise ConnectionError(f"Erro ao conectar ao banco de dados: {e}")
    
    def close(self) -> None:
        """Fecha a conexão com o banco de dados."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def parse_date(cls, s: str) -> date:
        """
        Converte string de data para objeto date.
        
        Args:
            s: String de data (formato ISO YYYY-MM-DD)
            
        Returns:
            Objeto date
        """
        return isoparse(s).date()
    
    def classificar_beancount(cls, clas_cta: str, tipo_cta: Optional[str]) -> str:
        """
        Classifica conta contábil em categoria Beancount.
        
        Args:
            clas_cta: Classificação da conta (ex: "1.1.01")
            tipo_cta: Tipo da conta (ex: "A" para Ativo, "P" para Passivo)
            
        Returns:
            Nome da categoria Beancount (Assets, Liabilities, etc.)
        """
        if tipo_cta:
            t = str(tipo_cta).strip().upper()
            if t == "A":  return "Assets"
            if t == "P":  return "Liabilities"
            if t in ("PL", "P/L", "PATRIMONIO", "PATRIMÔNIO"): return "Equity"
            if t.startswith("R"): return "Income"
            if t.startswith("D"): return "Expenses"
        clas = (clas_cta or "").strip()
        if clas.startswith("1."): return "Assets"
        if clas.startswith("2."): return "Liabilities"
        if clas.startswith("3."): return "Equity"
        if clas.startswith("4.") or clas.startswith("5."): return "Income"
        if clas.startswith("6.") or clas.startswith("7."): return "Expenses"
        return "Unknown"
    
    def normalizar_nome(cls, nome: str) -> str:
        """
        Normaliza nome da conta removendo acentos e caracteres especiais.
        
        Args:
            nome: Nome da conta original
            
        Returns:
            Nome normalizado para Beancount
        """
        if pd.isna(nome): 
            return "Sem_Nome"
        
        s = str(nome).strip()
        repl = {
            " ": "_", "/": "-", "&": "E",
            "ç": "c", "ã": "a", "á": "a", "à": "a", "â": "a",
            "é": "e", "ê": "e", "í": "i", "ó": "o", "ô": "o", "õ": "o", "ú": "u",
            "Ç": "C", "Ã": "A", "Á": "A", "À": "A", "Â": "A",
            "É": "E", "Ê": "E", "Í": "I", "Ó": "O", "Ô": "O", "Õ": "O", "Ú": "U",
        }
        for k, v in repl.items():
            s = s.replace(k, v)
        return s
    
    def buscar_plano_contas(self) -> pd.DataFrame:
        """
        Busca plano de contas do banco de dados e mapeia para Beancount.
        
        Returns:
            DataFrame com plano de contas e mapeamento para Beancount
        """
        sql_pc = """
        SELECT 
          CODI_EMP,
          CODI_CTA,
          NOME_CTA,
          CLAS_CTA,
          TIPO_CTA,
          SITUACAO_CTA
        FROM BETHADBA.CTCONTAS
        WHERE CODI_EMP = ?
        """
        
        df_pc = pd.read_sql(sql_pc, self.conn, params=[self.empresa])
        
        # Aplica classificação Beancount
        df_pc["BC_GROUP"] = [
            self.classificar_beancount(c, t) 
            for c, t in zip(df_pc["CLAS_CTA"], df_pc["TIPO_CTA"])
        ]
        
        # Normaliza nomes
        df_pc["BC_NAME"] = df_pc["NOME_CTA"].astype(str).apply(self.normalizar_nome)
        
        # Cria conta Beancount completa
        df_pc["BC_ACCOUNT"] = df_pc["BC_GROUP"] + ":" + df_pc["BC_NAME"]
        
        # Cria mapa para lookup
        self.mapa_clas_to_bc = dict(zip(df_pc["CLAS_CTA"].astype(str), df_pc["BC_ACCOUNT"]))
        
        self.df_pc = df_pc
        return df_pc
    
    def buscar_saldos(self) -> pd.DataFrame:
        """
        Busca saldos até a data de corte.
        
        Returns:
            DataFrame com saldos por conta
        """
        sql_saldos = """
        SELECT conta, SUM(valor) AS saldo
        FROM (
            SELECT l.cdeb_lan AS conta, SUM(l.vlor_lan) AS valor
              FROM BETHADBA.CTLANCTO l
             WHERE l.codi_emp = ?
               AND l.data_lan <= ?
             GROUP BY l.cdeb_lan
            UNION ALL
            SELECT l.ccre_lan AS conta, -SUM(l.vlor_lan) AS valor
              FROM BETHADBA.CTLANCTO l
             WHERE l.codi_emp = ?
               AND l.data_lan <= ?
             GROUP BY l.ccre_lan
        ) X
        GROUP BY conta
        HAVING SUM(valor) <> 0
        ORDER BY conta
        """
        
        df_saldos = pd.read_sql(
            sql_saldos, 
            self.conn, 
            params=[self.empresa, self.ate, self.empresa, self.ate]
        )
        
        # Normaliza nomes das colunas
        if "conta" not in df_saldos.columns:
            df_saldos.columns = [c.lower() for c in df_saldos.columns]
        
        df_saldos["conta"] = df_saldos["conta"].astype(str)
        
        self.df_saldos = df_saldos
        return df_saldos
    
    def processar_saldos(self) -> pd.DataFrame:
        """
        Processa saldos adicionando metadados (empresa, data_corte, BC_ACCOUNT).
        
        Returns:
            DataFrame processado com todas as colunas necessárias
        """
        if self.df_saldos is None or self.df_pc is None:
            raise ValueError("Plano de contas e saldos devem ser buscados primeiro.")
        
        # Junta classificação para facilitar auditoria
        df_result = self.df_saldos.merge(
            self.df_pc[["CLAS_CTA", "BC_ACCOUNT"]], 
            left_on="conta", 
            right_on="CLAS_CTA", 
            how="left"
        )
        
        # Adiciona metadados
        df_result["empresa"] = self.empresa
        df_result["data_corte"] = self.ate.isoformat()
        
        return df_result
    
    def salvar_csv(self, df: pd.DataFrame, out_path: Path) -> None:
        """
        Salva DataFrame em arquivo CSV.
        
        Args:
            df: DataFrame a ser salvo
            out_path: Caminho do arquivo de saída
        """
        cols = ["conta", "saldo", "CLAS_CTA", "BC_ACCOUNT", "empresa", "data_corte"]
        df[cols].to_csv(out_path, index=False, sep=";", encoding="utf-8-sig")
    
    def execute(self) -> Path:
        """
        Executa o processo completo de geração de saldos iniciais.
        
        Returns:
            Caminho do arquivo CSV gerado
        """
        # Prepara diretório de saída
        self.saida.mkdir(parents=True, exist_ok=True)
        out_path = self.saida / f"saldos_iniciais_{self.empresa}_{self.ate}.csv"
        
        try:
            # Conecta ao banco
            self.connect()
            
            # Busca plano de contas
            self.buscar_plano_contas()
            
            # Busca saldos
            self.buscar_saldos()
            
            # Processa e salva
            df_result = self.processar_saldos()
            self.salvar_csv(df_result, out_path)
            
            return out_path
            
        finally:
            # Sempre fecha a conexão
            self.close()


def carregar_config(config_path: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Carrega configuração de banco de dados de arquivo INI.
    
    Args:
        config_path: Caminho do arquivo de configuração
        
    Returns:
        Tupla com (dsn, user, password)
    """
    if not config_path:
        return None, None, None
    
    cfg = configparser.ConfigParser()
    cfg.read(config_path)
    
    dsn = cfg.get("database", "dsn", fallback=None)
    user = cfg.get("database", "user", fallback=None)
    password = cfg.get("database", "password", fallback=None)
    
    return dsn, user, password


def main():
    """Função principal para interface CLI."""
    ap = argparse.ArgumentParser(description="Constrói arquivo de saldos iniciais (cache).")
    ap.add_argument("--dsn", required=False, default=None)
    ap.add_argument("--user", required=False, default=None)
    ap.add_argument("--password", required=False, default=None)
    ap.add_argument("--empresa", type=int, required=True)
    ap.add_argument("--ate", required=True, help="Data de corte (YYYY-MM-DD)")
    ap.add_argument("--saida", default="./out")
    ap.add_argument("--config", default=None, help="Arquivo INI com [database] dsn/user/password (opcional)")
    args = ap.parse_args()

    # Parse da data
    ate = isoparse(args.ate).date()
    
    # Carrega credenciais (argumentos CLI têm prioridade sobre config file)
    dsn = args.dsn
    user = args.user
    password = args.password
    
    # Carrega config se fornecido
    if args.config:
        cfg_dsn, cfg_user, cfg_password = carregar_config(args.config)
        if not dsn: dsn = cfg_dsn
        if not user: user = cfg_user
        if not password: password = cfg_password

    # Valida credenciais
    if not all([dsn, user, password]):
        print("ERRO: Informe DSN/USER/PASSWORD via argumentos ou config.ini.", file=sys.stderr)
        sys.exit(1)

    # Executa o construtor
    builder = OpeningBalancesBuilder(
        dsn=dsn,
        user=user,
        password=password,
        empresa=args.empresa,
        ate=ate,
        saida=args.saida
    )
    
    try:
        out_path = builder.execute()
        print(f"OK: salvos saldos iniciais em {out_path.resolve()}")
    except Exception as e:
        print(f"ERRO: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
