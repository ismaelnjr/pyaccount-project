#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera arquivo de Saldos Iniciais (cache) até uma data de corte (D_corte),
para evitar varredura histórica a cada execução do pipeline principal.

Uso:
  # Busca saldo direto até data final
  python opening_balances.py \
      --dsn SQLANYWHERE17 --user dba --password sql \
      --empresa 437 --ate 2025-08-31 \
      --saida ./out

  # Calcula saldo usando saldos iniciais de arquivo + movimentações do período
  python opening_balances.py \
      --dsn SQLANYWHERE17 --user dba --password sql \
      --empresa 437 --saldos-iniciais saldos_2023-12-31.csv \
      --data-abertura 2023-12-31 --ate 2024-01-31 \
      --saida ./out

Saída:
  out/saldos_iniciais_<empresa>_<ate>.csv (sem saldos iniciais)
  out/saldos_iniciais_<empresa>_<data_abertura>_<ate>.csv (com saldos iniciais)
  (colunas: conta, NOME_CTA, BC_GROUP, saldo, CLAS_CTA, BC_ACCOUNT, empresa, data_corte)
"""
import argparse
import configparser
from pathlib import Path
from datetime import date
from typing import Optional, Dict, Union
import sys

import pandas as pd
from dateutil.parser import isoparse

from pyaccount.data.db_client import ContabilDBClient
from pyaccount.core.account_classifier import AccountClassifier, TipoPlanoContas, obter_classificacao_do_modelo
from pyaccount.core.account_mapper import AccountMapper
from pyaccount.builders.financial_statements import _FinancialStatementBase


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
        classificacao_customizada: Optional[Dict[str, str]] = None,
        modelo: Optional[TipoPlanoContas] = None,
        saldos_iniciais: Optional[Union[Dict[str, float], pd.DataFrame]] = None,
        data_abertura: Optional[date] = None
    ):
        """
        Inicializa o construtor de saldos iniciais.
        
        Args:
            dsn: Nome do DSN ODBC
            user: Usuário do banco de dados
            password: Senha do banco de dados
            empresa: Código da empresa
            ate: Data de corte (até quando calcular os saldos finais)
            saida: Diretório de saída dos arquivos
            classificacao_customizada: Dicionário opcional com mapeamento customizado de prefixos
                                      CLAS_CTA para categorias Beancount.
                                      Ex: {"1": "Assets", "2": "Liabilities", "31": "Expenses", ...}
                                      Se None, usa a configuração padrão.
            modelo: Tipo de plano de contas a usar (TipoPlanoContas.PADRAO_BR, SIMPLIFICADO, IFRS).
                    Se None, usa CLASSIFICACAO_PADRAO_BR. Se classificacao_customizada for fornecido,
                    tem prioridade sobre o modelo.
            saldos_iniciais: Saldos iniciais (de abertura) fornecidos externamente. Pode ser:
                            - Dict[str, float]: {"conta": saldo, ...} onde conta é o CODI_CTA
                            - pd.DataFrame: Deve conter coluna 'conta' (ou 'CODI_CTA') e 'saldo'
                            Se fornecido, será usado em vez de buscar do banco de dados.
            data_abertura: Data dos saldos iniciais (ex: 2023-12-31). Obrigatória se 
                          saldos_iniciais for fornecido. A data 'ate' deve ser maior que esta data.
        """
        self.db_client = ContabilDBClient(dsn, user, password)
        self.empresa = empresa
        self.ate = ate
        self.saida = Path(saida)
        self.classificacao_customizada = classificacao_customizada
        self.saldos_iniciais = saldos_iniciais
        self.data_abertura = data_abertura
        
        # Validações
        if saldos_iniciais is not None and data_abertura is None:
            raise ValueError("data_abertura é obrigatória quando saldos_iniciais é fornecido")
        
        if data_abertura is not None:
            if data_abertura >= ate:
                raise ValueError(
                    f"data_abertura ({data_abertura}) deve ser anterior à data final 'ate' ({ate})"
                )
        
        # Obtém classificação baseada no modelo e customizações
        classificacao = obter_classificacao_do_modelo(modelo, classificacao_customizada)
        
        # Mapeador de contas (classe base compartilhada)
        self.account_mapper = AccountMapper(classificacao)
        
        # DataFrames internos
        self.df_pc: Optional[pd.DataFrame] = None
        self.df_saldos: Optional[pd.DataFrame] = None
        self.mapa_clas_to_bc: Dict[str, str] = {}
    
    def classificar_beancount(self, clas_cta: str, tipo_cta: Optional[str]) -> str:
        """
        Classifica conta contábil em categoria Beancount baseado em CLAS_CTA.
        
        Args:
            clas_cta: Classificação da conta (ex: "11210100708", "311203", "4")
            tipo_cta: Tipo da conta ('A' = analítica, 'S' = sintética) - não usado para classificação
            
        Returns:
            Nome da categoria Beancount (Assets, Liabilities, Income, Expenses, etc.)
        """
        return self.account_mapper.classificar_beancount(clas_cta, tipo_cta)
    
    @staticmethod
    def normalizar_saldos_iniciais(
        saldos_iniciais: Union[Dict[str, float], pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Normaliza saldos iniciais fornecidos para formato DataFrame padronizado.
        
        Args:
            saldos_iniciais: Dict[str, float] ou pd.DataFrame com saldos iniciais
            
        Returns:
            DataFrame com colunas: conta (str), saldo (float)
        """
        if isinstance(saldos_iniciais, dict):
            # Converte dicionário para DataFrame
            df = pd.DataFrame([
                {"conta": str(conta), "saldo": float(saldo)}
                for conta, saldo in saldos_iniciais.items()
            ])
        elif isinstance(saldos_iniciais, pd.DataFrame):
            df = saldos_iniciais.copy()
            
            # Tenta encontrar coluna de conta (pode ser 'conta', 'CODI_CTA', etc.)
            coluna_conta = None
            for col in ["conta", "CODI_CTA", "codi_cta", "Conta", "codigo"]:
                if col in df.columns:
                    coluna_conta = col
                    break
            
            if coluna_conta is None:
                raise ValueError(
                    "DataFrame de saldos iniciais deve conter coluna 'conta' ou 'CODI_CTA'"
                )
            
            # Tenta encontrar coluna de saldo
            coluna_saldo = None
            for col in ["saldo", "Saldo", "SALDO", "valor", "Valor"]:
                if col in df.columns:
                    coluna_saldo = col
                    break
            
            if coluna_saldo is None:
                raise ValueError(
                    "DataFrame de saldos iniciais deve conter coluna 'saldo'"
                )
            
            # Extrai apenas colunas necessárias e renomeia
            df = df[[coluna_conta, coluna_saldo]].copy()
            df.columns = ["conta", "saldo"]
            
            # Converte tipos
            df["conta"] = df["conta"].astype(str)
            # Converte saldo para numérico, tratando possíveis formatos (vírgula ou ponto como decimal)
            df["saldo"] = df["saldo"].astype(str).str.replace(",", ".", regex=False)
            df["saldo"] = pd.to_numeric(df["saldo"], errors="coerce").fillna(0.0)
            
            # Arredonda para 2 casas decimais para evitar problemas de precisão
            df["saldo"] = df["saldo"].round(2)
            
            # Remove linhas com saldo zero
            df = df[df["saldo"] != 0].copy()
        else:
            raise TypeError(
                "saldos_iniciais deve ser Dict[str, float] ou pd.DataFrame"
            )
        
        return df
    
    def buscar_plano_contas(self) -> pd.DataFrame:
        """
        Busca plano de contas do banco de dados e mapeia para Beancount.
        
        Returns:
            DataFrame com plano de contas e mapeamento para Beancount
        """
        # Busca plano de contas usando o cliente de banco de dados
        df_pc = self.db_client.buscar_plano_contas(self.empresa)
        
        # Processa plano de contas usando AccountMapper
        df_pc = self.account_mapper.processar_plano_contas(df_pc, filtrar_ativas=False)
        
        # Cria mapa para lookup
        mapas = self.account_mapper.criar_mapas(df_pc)
        self.mapa_clas_to_bc = mapas["clas_to_bc"]
        
        self.df_pc = df_pc
        return df_pc
    
    def buscar_saldos(self) -> pd.DataFrame:
        """
        Busca saldos finais. Se saldos_iniciais foram fornecidos, calcula:
        saldo_final = saldo_inicial + movimentações_período.
        Caso contrário, busca saldo direto até 'ate'.
        
        Returns:
            DataFrame com saldos por conta
        """
        if self.saldos_iniciais is not None and self.data_abertura is not None:
            # Usa saldos iniciais fornecidos externamente
            df_saldo_inicial = self.normalizar_saldos_iniciais(self.saldos_iniciais)
            
            # Busca movimentações do período entre data_abertura e ate
            # Período: data_abertura (exclusiva) até ate (inclusiva)
            # Ex: abertura em 31/12/2023, movimentações de 01/01/2024 a 31/01/2024
            df_movimentacoes = self.db_client.buscar_movimentacoes_periodo(
                self.empresa, self.data_abertura, self.ate
            )
            
            # Prepara DataFrames para merge
            if df_movimentacoes.empty:
                # Se não há movimentações, mantém apenas saldo inicial
                df_saldos = df_saldo_inicial.copy()
            else:
                # Prepara índices para merge
                if not df_saldo_inicial.empty:
                    df_saldo_inicial = df_saldo_inicial.copy()
                    df_saldo_inicial["conta"] = df_saldo_inicial["conta"].astype(str)
                    df_saldo_inicial.set_index("conta", inplace=True)
                else:
                    # Cria DataFrame vazio com índice para fazer join
                    df_saldo_inicial = pd.DataFrame(columns=["saldo"])
                
                df_movimentacoes = df_movimentacoes.copy()
                df_movimentacoes["conta"] = df_movimentacoes["conta"].astype(str)
                df_movimentacoes.set_index("conta", inplace=True)
                
                # Combina saldo inicial e movimentações usando outer join
                # Se conta não tem movimentação, movimento = 0
                # Se conta não tem saldo inicial, saldo inicial = 0
                df_saldos = df_saldo_inicial.join(
                    df_movimentacoes, 
                    how="outer"
                )
        
                # Preenche NaN com 0
                df_saldos["saldo"] = df_saldos["saldo"].fillna(0)
                if "movimento" in df_saldos.columns:
                    df_saldos["movimento"] = df_saldos["movimento"].fillna(0)
                    # Calcula saldo final = saldo inicial + movimentações
                    df_saldos["saldo"] = df_saldos["saldo"] + df_saldos["movimento"]
                
                # Arredonda saldo para 2 casas decimais para evitar problemas de precisão
                df_saldos["saldo"] = df_saldos["saldo"].round(2)
                
                # Remove linhas com saldo zero
                df_saldos = df_saldos[df_saldos["saldo"] != 0].copy()
                
                # Reseta índice para ter coluna 'conta'
                df_saldos.reset_index(inplace=True)
                # Mantém apenas colunas conta e saldo
                df_saldos = df_saldos[["conta", "saldo"]].copy()
        else:
            # Busca saldos direto até 'ate' (comportamento original)
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
        
        # Usa método auxiliar para merge
        df_result = _FinancialStatementBase._merge_com_plano_contas(
            self.df_saldos,
            self.df_pc,
            coluna_conta_df="conta",
            coluna_conta_pc="CODI_CTA",
            colunas_pc=["CODI_CTA", "CLAS_CTA", "NOME_CTA", "BC_GROUP", "BC_ACCOUNT"]
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
        # Prepara cópia para não modificar o DataFrame original
        df_salvar = df.copy()
        
        # Arredonda saldo para 2 casas decimais antes de salvar
        if "saldo" in df_salvar.columns:
            df_salvar["saldo"] = df_salvar["saldo"].round(2)
        
        # Define ordem das colunas: conta, descrição, classificação, saldo, conta Beancount, empresa, data
        cols = ["conta", "NOME_CTA", "BC_GROUP", "saldo", "CLAS_CTA", "BC_ACCOUNT", "empresa", "data_corte"]
        
        # Filtra apenas colunas que existem no DataFrame
        cols_existentes = [col for col in cols if col in df_salvar.columns]
        df_salvar[cols_existentes].to_csv(out_path, index=False, sep=";", encoding="utf-8-sig", float_format="%.2f")
    
    def execute(self) -> Path:
        """
        Executa o processo completo de geração de saldos iniciais.
        
        Returns:
            Caminho do arquivo CSV gerado
        """
        # Prepara diretório de saída
        self.saida.mkdir(parents=True, exist_ok=True)
        
        # Define nome do arquivo de saída
        if self.data_abertura is not None:
            out_path = self.saida / f"saldos_iniciais_{self.empresa}_{self.data_abertura}_{self.ate}.csv"
        else:
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


# Funções utilitárias para CLI
def carregar_saldos_iniciais_de_arquivo(
    caminho_arquivo: Union[str, Path], 
    separador: str = ";",
    encoding: str = "utf-8-sig"
) -> pd.DataFrame:
    """
    Carrega saldos iniciais de um arquivo CSV.
    
    Args:
        caminho_arquivo: Caminho do arquivo CSV com saldos iniciais
        separador: Separador do CSV (default: ";")
        encoding: Encoding do arquivo (default: "utf-8-sig")
    
    Returns:
        DataFrame com colunas: conta, saldo
        
    O arquivo deve conter pelo menos as colunas:
    - conta (ou CODI_CTA, codi_cta, Conta, codigo): Código da conta
    - saldo (ou Saldo, SALDO, valor, Valor): Saldo da conta
    """
    arquivo = Path(caminho_arquivo)
    if not arquivo.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {arquivo}")
    
    df = pd.read_csv(arquivo, sep=separador, encoding=encoding)
    
    # Normaliza usando o método estático
    return OpeningBalancesBuilder.normalizar_saldos_iniciais(df)


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
    ap.add_argument("--ate", required=True, help="Data final para cálculo do saldo (YYYY-MM-DD)")
    ap.add_argument("--saldos-iniciais", required=False, default=None, help="Caminho para arquivo CSV com saldos iniciais. Deve conter colunas 'conta' e 'saldo'")
    ap.add_argument("--data-abertura", required=False, default=None, help="Data dos saldos iniciais (YYYY-MM-DD). Obrigatória se --saldos-iniciais for fornecido")
    ap.add_argument("--saida", default="./out")
    ap.add_argument("--config", default=None, help="Arquivo INI com [database] dsn/user/password (opcional)")
    args = ap.parse_args()

    # Parse das datas
    ate = isoparse(args.ate).date()
    data_abertura = isoparse(args.data_abertura).date() if args.data_abertura else None
    
    # Validações
    if args.saldos_iniciais and not data_abertura:
        print("ERRO: --data-abertura é obrigatória quando --saldos-iniciais é fornecido.", file=sys.stderr)
        sys.exit(1)
    
    if data_abertura is not None and data_abertura >= ate:
        print(f"ERRO: data-abertura ({data_abertura}) deve ser anterior à data final --ate ({ate}).", file=sys.stderr)
        sys.exit(1)
    
    # Carrega credenciais (argumentos CLI têm prioridade sobre config file)
    dsn = args.dsn
    user = args.user
    password = args.password
    
    # Carrega saldos iniciais se fornecido
    saldos_iniciais = None
    if args.saldos_iniciais:
        saldos_iniciais = carregar_saldos_iniciais_de_arquivo(args.saldos_iniciais)
    
    # Carrega config se fornecido
    classificacao_customizada = None
    if args.config:
        cfg_dsn, cfg_user, cfg_password = carregar_config(args.config)
        if not dsn: dsn = cfg_dsn
        if not user: user = cfg_user
        if not password: password = cfg_password
        
        # Carrega classificação customizada se houver
        classifier = AccountClassifier.carregar_do_ini(args.config)
        classificacao_customizada = classifier.mapeamento if classifier else None

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
        classificacao_customizada=classificacao_customizada,
        saldos_iniciais=saldos_iniciais,
        data_abertura=data_abertura
    )
    
    try:
        out_path = builder.execute()
        print(f"OK: salvos saldos iniciais em {out_path.resolve()}")
    except Exception as e:
        print(f"ERRO: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

