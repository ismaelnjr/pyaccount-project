from __future__ import annotations
from datetime import date
from typing import Optional
import sqlite3
import pandas as pd

from pyaccount.data.client import DataClient  # sua interface base
from pyaccount.data.logging import log_query

class SQLiteClient(DataClient):
    """
    Implementa DataClient usando um arquivo SQLite.
    Requisitos de esquema estão em pyaccount/data/sql/schema.sql
    """

    def __init__(self, db_path: str, enable_query_log: bool = False, query_log_file: str = "logs/queries.log"):
        """
        Inicializa o cliente SQLite.
        
        Args:
            db_path: Caminho do arquivo SQLite
            enable_query_log: Se True, registra todas as queries SQL em arquivo de log
            query_log_file: Caminho do arquivo de log (padrão: logs/queries.log)
        """
        self.db_path = db_path
        self.enable_query_log = enable_query_log
        self.query_log_file = query_log_file

    def _con(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        con.row_factory = sqlite3.Row
        return con

    # ---- API prevista pelo DataClient ----
    def buscar_plano_contas(self, empresa: int) -> pd.DataFrame:
        sql = "SELECT * FROM plano_contas WHERE codi_emp = ?"
        if self.enable_query_log:
            log_query(sql, [empresa], self.query_log_file)
        df = pd.read_sql(sql, self._con(), params=[empresa])
        # Normaliza nomes das colunas para maiúsculas (padrão esperado pelos builders)
        df.columns = df.columns.str.upper()
        # Se bc_group não existir (bancos antigos), retorna sem ela (será calculado depois)
        return df

    def buscar_saldos(self, empresa: int, ate: date) -> pd.DataFrame:
        con = self._con()

        sql_mov = """
        SELECT conta,
               SUM(CASE WHEN lado='D' THEN valor ELSE 0 END)
             - SUM(CASE WHEN lado='C' THEN valor ELSE 0 END) AS movimento
          FROM lancamentos
         WHERE codi_emp = ? AND date(data_lan) <= date(?)
         GROUP BY conta
        """
        if self.enable_query_log:
            log_query(sql_mov, [empresa, ate], self.query_log_file)
        df_mov = pd.read_sql(sql_mov, con, params=[empresa, ate])

        # último saldo inicial <= data por conta
        sql_si = """
        WITH ult AS (
          SELECT codi_emp, conta, MAX(date(data_saldo)) AS dref
            FROM saldos_iniciais
           WHERE codi_emp = ? AND date(data_saldo) <= date(?)
           GROUP BY codi_emp, conta
        )
        SELECT s.conta, s.saldo
          FROM saldos_iniciais s
          JOIN ult u
            ON s.codi_emp=u.codi_emp AND s.conta=u.conta AND date(s.data_saldo)=u.dref
        """
        if self.enable_query_log:
            log_query(sql_si, [empresa, ate], self.query_log_file)
        df_si = pd.read_sql(sql_si, con, params=[empresa, ate])

        df = pd.merge(df_si, df_mov, how="outer", on="conta")
        df["saldo"] = pd.to_numeric(df["saldo"], errors="coerce").fillna(0.0)
        df["movimento"] = pd.to_numeric(df["movimento"], errors="coerce").fillna(0.0)
        df["saldo"] = df["saldo"] + df["movimento"]
        return df[["conta", "saldo"]]

    def buscar_lancamentos_periodo(self, empresa: int, inicio: date, fim: date) -> pd.DataFrame:
        sql = """
        SELECT *
          FROM lancamentos
         WHERE codi_emp = ?
           AND date(data_lan) >= date(?)
           AND date(data_lan) <= date(?)
         ORDER BY date(data_lan), codi_lote, nume_lan, CASE lado WHEN 'D' THEN 0 ELSE 1 END
        """
        if self.enable_query_log:
            log_query(sql, [empresa, inicio, fim], self.query_log_file)
        df = pd.read_sql(sql, self._con(), params=[empresa, inicio, fim])
        
        # Converte formato SQLite (lado + conta) para formato esperado pelos builders (cdeb_lan + ccre_lan)
        if not df.empty and "lado" in df.columns and "conta" in df.columns:
            # Garante que conta seja string
            df["conta"] = df["conta"].astype(str)
            # Cria colunas cdeb_lan e ccre_lan baseadas no lado
            df["cdeb_lan"] = df.apply(lambda row: str(row["conta"]) if row["lado"] == "D" else "0", axis=1)
            df["ccre_lan"] = df.apply(lambda row: str(row["conta"]) if row["lado"] == "C" else "0", axis=1)
            df["vlor_lan"] = df["valor"]
            # Remove colunas originais que não são esperadas
            df = df.drop(columns=["lado"], errors="ignore")
        
        return df

    def buscar_movimentacoes_periodo(self, empresa: int, de: date, ate: date) -> pd.DataFrame:
        sql = """
        SELECT conta,
               SUM(CASE WHEN lado='D' THEN valor ELSE 0 END)
             - SUM(CASE WHEN lado='C' THEN valor ELSE 0 END) AS movimento
          FROM lancamentos
         WHERE codi_emp = ?
           AND date(data_lan) > date(?)
           AND date(data_lan) <= date(?)
         GROUP BY conta
        """
        if self.enable_query_log:
            log_query(sql, [empresa, de, ate], self.query_log_file)
        return pd.read_sql(sql, self._con(), params=[empresa, de, ate])
    
    def listar_empresas(self) -> pd.DataFrame:
        """
        Lista todas as empresas cadastradas.
        
        Returns:
            DataFrame com colunas CODI_EMP e NOME, ordenado por CODI_EMP
        """
        sql = "SELECT CODI_EMP, NOME FROM empresas ORDER BY CODI_EMP"
        if self.enable_query_log:
            log_query(sql, None, self.query_log_file)
        df = pd.read_sql(sql, self._con())
        # Normaliza nomes das colunas para maiúsculas
        df.columns = df.columns.str.upper()
        return df
