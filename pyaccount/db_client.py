#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cliente ODBC para consultas ao banco de dados contábil.

Esta classe encapsula toda a lógica de conexão e consultas SQL via ODBC,
separando as responsabilidades de acesso a dados da lógica de negócio.
"""
from datetime import date
from typing import Optional
import pyodbc
import pandas as pd


class ContabilDBClient:
    """
    Cliente para executar consultas SQL via ODBC no banco de dados contábil.
    
    Esta classe gerencia a conexão ODBC e fornece métodos especializados
    para consultas específicas do sistema contábil.
    """
    
    def __init__(self, dsn: str, user: str, password: str):
        """
        Inicializa o cliente de banco de dados.
        
        Args:
            dsn: Nome do DSN ODBC
            user: Usuário do banco de dados
            password: Senha do banco de dados
        """
        self.dsn = dsn
        self.user = user
        self.password = password
        self.conn: Optional[pyodbc.Connection] = None
    
    def connect(self) -> None:
        """
        Estabelece conexão com o banco de dados via ODBC.
        
        Raises:
            ValueError: Se DSN, user ou password não forem fornecidos
            ConnectionError: Se houver erro ao conectar
        """
        if not all([self.dsn, self.user, self.password]):
            raise ValueError("DSN, user e password devem ser fornecidos.")
        
        try:
            connection_string = f"DSN={self.dsn};UID={self.user};PWD={self.password}"
            self.conn = pyodbc.connect(connection_string)
        except Exception as e:
            raise ConnectionError(f"Erro ao conectar ao banco de dados: {e}")
    
    def close(self) -> None:
        """Fecha a conexão com o banco de dados."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def is_connected(self) -> bool:
        """
        Verifica se está conectado ao banco de dados.
        
        Returns:
            True se conectado, False caso contrário
        """
        return self.conn is not None
    
    def buscar_plano_contas(self, empresa: int) -> pd.DataFrame:
        """
        Busca plano de contas do banco de dados para uma empresa específica.
        
        Args:
            empresa: Código da empresa
            
        Returns:
            DataFrame com colunas: CODI_EMP, CODI_CTA, NOME_CTA, CLAS_CTA, 
                                  TIPO_CTA, SITUACAO_CTA
            
        Raises:
            RuntimeError: Se não estiver conectado ao banco de dados
        """
        if not self.is_connected():
            raise RuntimeError("Não está conectado ao banco de dados. Chame connect() primeiro.")
        
        sql = """
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
        
        df = pd.read_sql(sql, self.conn, params=[empresa])
        return df
    
    def buscar_saldos(self, empresa: int, ate: date) -> pd.DataFrame:
        """
        Busca saldos até uma data de corte específica.
        
        Args:
            empresa: Código da empresa
            ate: Data de corte (até quando calcular os saldos)
            
        Returns:
            DataFrame com colunas: conta, saldo (com nomes de colunas em minúsculas)
            
        Raises:
            RuntimeError: Se não estiver conectado ao banco de dados
        """
        if not self.is_connected():
            raise RuntimeError("Não está conectado ao banco de dados. Chame connect() primeiro.")
        
        sql = """
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
        
        df = pd.read_sql(
            sql, 
            self.conn, 
            params=[empresa, ate, empresa, ate]
        )
        
        # Normaliza nomes das colunas para minúsculas
        if df.columns.size > 0:
            if "conta" not in df.columns:
                df.columns = [c.lower() for c in df.columns]
            df["conta"] = df["conta"].astype(str)
        
        return df
    
    def buscar_movimentacoes_periodo(self, empresa: int, de: date, ate: date) -> pd.DataFrame:
        """
        Busca movimentações (débitos e créditos) de um período específico.
        
        Args:
            empresa: Código da empresa
            de: Data inicial do período (exclusiva - movimentações após esta data)
            ate: Data final do período (inclusive - movimentações até esta data)
            
        Returns:
            DataFrame com colunas: conta, movimento (com nomes de colunas em minúsculas)
            movimento = débitos - créditos (valor positivo aumenta saldo, negativo diminui)
            
        Raises:
            RuntimeError: Se não estiver conectado ao banco de dados
        """
        if not self.is_connected():
            raise RuntimeError("Não está conectado ao banco de dados. Chame connect() primeiro.")
        
        sql = """
        SELECT conta, SUM(valor) AS movimento
        FROM (
            SELECT l.cdeb_lan AS conta, l.vlor_lan AS valor
              FROM BETHADBA.CTLANCTO l
             WHERE l.codi_emp = ?
               AND l.data_lan > ?
               AND l.data_lan <= ?
            UNION ALL
            SELECT l.ccre_lan AS conta, -l.vlor_lan AS valor
              FROM BETHADBA.CTLANCTO l
             WHERE l.codi_emp = ?
               AND l.data_lan > ?
               AND l.data_lan <= ?
        ) X
        GROUP BY conta
        HAVING SUM(valor) <> 0
        ORDER BY conta
        """
        
        df = pd.read_sql(
            sql, 
            self.conn, 
            params=[empresa, de, ate, empresa, de, ate]
        )
        
        # Normaliza nomes das colunas para minúsculas
        if df.columns.size > 0:
            if "conta" not in df.columns:
                df.columns = [c.lower() for c in df.columns]
            df["conta"] = df["conta"].astype(str)
        
        return df
    
    def buscar_lancamentos_periodo(self, empresa: int, inicio: date, fim: date) -> pd.DataFrame:
        """
        Busca lançamentos contábeis de um período específico.
        
        Args:
            empresa: Código da empresa
            inicio: Data inicial do período (inclusive)
            fim: Data final do período (inclusive)
            
        Returns:
            DataFrame com colunas: codi_emp, nume_lan, data_lan, vlor_lan,
                                  cdeb_lan, ccre_lan, codi_his, chis_lan,
                                  ndoc_lan, codi_lote, tipo, codi_usu
            
        Raises:
            RuntimeError: Se não estiver conectado ao banco de dados
        """
        if not self.is_connected():
            raise RuntimeError("Não está conectado ao banco de dados. Chame connect() primeiro.")
        
        sql = """
        SELECT  
         l.codi_emp,
         l.nume_lan, 
         l.data_lan, 
         l.vlor_lan,
         l.cdeb_lan,
         l.ccre_lan,
         l.codi_his,
         l.chis_lan,
         l.ndoc_lan,
         l.codi_lote,
         t.tipo,
         l.codi_usu
        FROM 
         BETHADBA.CTLANCTO l
         JOIN BETHADBA.CTLANCTOLOTE t
           ON l.codi_emp = t.codi_emp
          AND l.codi_lote = t.codi_lote
        WHERE 
         l.codi_emp = ?
         AND l.data_lan BETWEEN ? AND ?
        ORDER BY l.data_lan, l.nume_lan
        """
        
        df = pd.read_sql(sql, self.conn, params=[empresa, inicio, fim])
        
        # Normaliza nomes das colunas para minúsculas
        if df.columns.size > 0:
            if "conta" not in df.columns:
                df.columns = [c.lower() for c in df.columns]
        
        return df
    
    def executar_query(self, sql: str, params: Optional[list] = None) -> pd.DataFrame:
        """
        Executa uma query SQL genérica e retorna um DataFrame.
        
        Args:
            sql: Query SQL a ser executada
            params: Lista de parâmetros para a query (opcional)
            
        Returns:
            DataFrame com os resultados da query
            
        Raises:
            RuntimeError: Se não estiver conectado ao banco de dados
        """
        if not self.is_connected():
            raise RuntimeError("Não está conectado ao banco de dados. Chame connect() primeiro.")
        
        if params:
            return pd.read_sql(sql, self.conn, params=params)
        else:
            return pd.read_sql(sql, self.conn)
    
    def __enter__(self):
        """Suporte para context manager (with statement)."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Suporte para context manager (with statement)."""
        self.close()

