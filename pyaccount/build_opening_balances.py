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
  (colunas: conta, NOME_CTA, BC_GROUP, saldo, CLAS_CTA, BC_ACCOUNT, empresa, data_corte)
"""
import argparse
import configparser
from pathlib import Path
from datetime import date
from typing import Optional, Dict
import sys

import pandas as pd
from dateutil.parser import isoparse

from pyaccount.db_client import ContabilDBClient
from pyaccount.classificacao import (
    classificar_conta, 
    CLASSIFICACAO_M1,
    carregar_classificacao_do_ini
)


class OpeningBalancesBuilder:
    """
    Constrói arquivos de saldos iniciais para Beancount a partir de banco de dados.
    
    Esta classe encapsula a lógica de negócio necessária para:
    - Buscar plano de contas via ContabilDBClient
    - Buscar saldos até uma data específica via ContabilDBClient
    - Mapear contas do sistema contábil para Beancount
    - Exportar resultados em CSV
    
    A conexão com o banco de dados é gerenciada pela classe ContabilDBClient.
    """
    
    def __init__(
        self, 
        dsn: str, 
        user: str, 
        password: str, 
        empresa: int, 
        ate: date, 
        saida: Path,
        classificacao_customizada: Optional[Dict[str, str]] = None
    ):
        """
        Inicializa o construtor de saldos iniciais.
        
        Args:
            dsn: Nome do DSN ODBC
            user: Usuário do banco de dados
            password: Senha do banco de dados
            empresa: Código da empresa
            ate: Data de corte (até quando calcular os saldos)
            saida: Diretório de saída dos arquivos
            classificacao_customizada: Dicionário opcional com mapeamento customizado de prefixos
                                      CLAS_CTA para categorias Beancount.
                                      Ex: {"1": "Assets", "2": "Liabilities", "31": "Expenses", ...}
                                      Se None, usa a configuração padrão.
        """
        self.db_client = ContabilDBClient(dsn, user, password)
        self.empresa = empresa
        self.ate = ate
        self.saida = Path(saida)
        self.classificacao_customizada = classificacao_customizada
        self.df_pc: Optional[pd.DataFrame] = None
        self.df_saldos: Optional[pd.DataFrame] = None
        self.mapa_clas_to_bc: Dict[str, str] = {}
    
    
    def parse_date(cls, s: str) -> date:
        """
        Converte string de data para objeto date.
        
        Args:
            s: String de data (formato ISO YYYY-MM-DD)
            
        Returns:
            Objeto date
        """
        return isoparse(s).date()
    
    def classificar_beancount(self, clas_cta: str, tipo_cta: Optional[str]) -> str:
        """
        Classifica conta contábil em categoria Beancount baseado em CLAS_CTA.
        
        Usa a configuração customizada fornecida no construtor, ou a configuração padrão.
        
        Args:
            clas_cta: Classificação da conta (ex: "11210100708", "311203", "4")
            tipo_cta: Tipo da conta ('A' = analítica, 'S' = sintética) - não usado para classificação
            
        Returns:
            Nome da categoria Beancount (Assets, Liabilities, Income, Expenses, etc.)
        """
        return classificar_conta(
            clas_cta, 
            tipo_cta, 
            self.classificacao_customizada
        )
    
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
        # Busca plano de contas usando o cliente de banco de dados
        df_pc = self.db_client.buscar_plano_contas(self.empresa)
        
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
        # Busca saldos usando o cliente de banco de dados
        df_saldos = self.db_client.buscar_saldos(self.empresa, self.ate)
        
        self.df_saldos = df_saldos
        return df_saldos
    
    def processar_saldos(self) -> pd.DataFrame:
        """
        Processa saldos adicionando metadados (empresa, data_corte, BC_ACCOUNT, NOME_CTA, BC_GROUP).
        
        Returns:
            DataFrame processado com todas as colunas necessárias
        """
        if self.df_saldos is None or self.df_pc is None:
            raise ValueError("Plano de contas e saldos devem ser buscados primeiro.")
        
        # Garante que os tipos sejam compatíveis para o merge
        df_saldos_merge = self.df_saldos.copy()
        df_pc_merge = self.df_pc[["CODI_CTA", "CLAS_CTA", "NOME_CTA", "BC_GROUP", "BC_ACCOUNT"]].copy()
        
        # Converte ambos para o mesmo tipo (string ou int)
        df_saldos_merge["conta"] = df_saldos_merge["conta"].astype(str)
        df_pc_merge["CODI_CTA"] = df_pc_merge["CODI_CTA"].astype(str)
        
        # Junta informações do plano de contas usando CODI_CTA (campo 'conta' corresponde a CODI_CTA)
        df_result = df_saldos_merge.merge(
            df_pc_merge, 
            left_on="conta", 
            right_on="CODI_CTA", 
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
        # Define ordem das colunas: conta, descrição, classificação, saldo, conta Beancount, empresa, data
        cols = ["conta", "NOME_CTA", "BC_GROUP", "saldo", "CLAS_CTA", "BC_ACCOUNT", "empresa", "data_corte"]
        
        # Filtra apenas colunas que existem no DataFrame
        cols_existentes = [col for col in cols if col in df.columns]
        df[cols_existentes].to_csv(out_path, index=False, sep=";", encoding="utf-8-sig")
    
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
            self.db_client.connect()
            
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
            self.db_client.close()


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
    classificacao_customizada = None
    if args.config:
        cfg_dsn, cfg_user, cfg_password = carregar_config(args.config)
        if not dsn: dsn = cfg_dsn
        if not user: user = cfg_user
        if not password: password = cfg_password
        
        # Carrega classificação customizada se houver
        classificacao_customizada = carregar_classificacao_do_ini(args.config)

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
        saida=args.saida,
        classificacao_customizada=classificacao_customizada
    )
    
    try:
        out_path = builder.execute()
        print(f"OK: salvos saldos iniciais em {out_path.resolve()}")
    except Exception as e:
        print(f"ERRO: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
