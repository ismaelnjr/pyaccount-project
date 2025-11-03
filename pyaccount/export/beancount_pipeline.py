#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline: ODBC (SQL Anywhere/Betha) -> Beancount
Gera .beancount com:
- open das contas
- transação de abertura (saldos até D-1)
- lançamentos do período (partidas dobradas)
Também exporta CSV do balancete de abertura e mapa CLAS_CTA -> conta Beancount.

Uso:
  python beancount_pipeline.py \
      --dsn SQLANYWHERE17 --user dba --password sql \
      --empresa 437 --inicio 2025-09-01 --fim 2025-09-18 \
      --saida ./out --moeda BRL

Requisitos: pyodbc, pandas, python-dateutil
"""
import argparse
import configparser
import sys
from pathlib import Path
from datetime import date, timedelta
from typing import Optional, Dict

import pandas as pd
from dateutil.parser import isoparse

from pyaccount.data.db_client import ContabilDBClient
from pyaccount.core.account_classifier import AccountClassifier
from pyaccount.core.account_mapper import AccountMapper
from pyaccount.export.exporters import BeancountExporter
from pyaccount.core.utils import normalizar_nome, format_val, fmt_amount


class BeancountPipeline:
    """
    Pipeline para extrair dados contábeis via ODBC e gerar arquivo Beancount.
    
    Esta classe encapsula a lógica de negócio necessária para:
    - Buscar plano de contas e mapear para Beancount
    - Buscar saldos de abertura (ou carregar de CSV cache)
    - Buscar lançamentos do período
    - Gerar arquivo Beancount formatado
    - Exportar CSVs auxiliares (mapa de contas, balancete de abertura)
    
    A conexão com o banco de dados é gerenciada pela classe ContabilDBClient.
    """
    
    def __init__(
        self,
    dsn: str,
    user: str,
    password: str,
    empresa: int,
    inicio: date,
    fim: date,
    moeda: str,
    outdir: Path,
    somente_ativas: bool = False,
    abrir_equity_abertura: str = "Equity:Abertura",
        saldos_path: Optional[str] = None,
        classificacao_customizada: Optional[Dict[str, str]] = None,
        desconsiderar_zeramento: bool = True,
    ):
        """
        Inicializa o pipeline Beancount.
        
        Args:
            dsn: Nome do DSN ODBC
            user: Usuário do banco de dados
            password: Senha do banco de dados
            empresa: Código da empresa
            inicio: Data inicial do período
            fim: Data final do período
            moeda: Código da moeda para Beancount (ex: "BRL")
            outdir: Diretório de saída dos arquivos
            somente_ativas: Se True, usa apenas contas com SITUACAO_CTA = 'A'
            abrir_equity_abertura: Nome da conta Equity para transação de abertura
            saldos_path: Caminho opcional para CSV de saldos de abertura (cache)
            classificacao_customizada: Dicionário opcional com mapeamento customizado
                                      de prefixos CLAS_CTA para categorias Beancount
            desconsiderar_zeramento: Se True, exclui lançamentos com orig_lan = 2 (Zeramento)
        """
        self.db_client = ContabilDBClient(dsn, user, password)
        self.empresa = empresa
        self.inicio = inicio
        self.fim = fim
        self.moeda = moeda
        self.outdir = Path(outdir)
        self.somente_ativas = somente_ativas
        self.abrir_equity_abertura = abrir_equity_abertura
        self.saldos_path = saldos_path
        self.classificacao_customizada = classificacao_customizada
        self.desconsiderar_zeramento = desconsiderar_zeramento
        
        # Mapeador de contas (classe base compartilhada)
        self.account_mapper = AccountMapper(classificacao_customizada)
        
        # DataFrames internos
        self.df_pc: Optional[pd.DataFrame] = None
        self.df_saldos: Optional[pd.DataFrame] = None
        self.df_lanc: Optional[pd.DataFrame] = None
        self.mapa_clas_to_bc: Dict[str, str] = {}
        self.mapa_codi_to_bc: Dict[str, str] = {}
    
    def classificar_beancount(self, clas_cta: str, tipo_cta: Optional[str] = None) -> str:
        """
        Mapeia CLAS_CTA -> grupo Beancount.
        
        Args:
            clas_cta: Classificação da conta
            tipo_cta: Tipo da conta ('A' = analítica, 'S' = sintética)
            
        Returns:
            Nome da categoria Beancount
        """
        return self.account_mapper.classificar_beancount(clas_cta, tipo_cta)
    
    def buscar_plano_contas(self) -> pd.DataFrame:
        """
        Busca plano de contas do banco de dados e mapeia para Beancount.
        
        Returns:
            DataFrame com plano de contas e mapeamento para Beancount
        """
        df_pc = self.db_client.buscar_plano_contas(self.empresa)
        
        if df_pc.empty:
            raise RuntimeError("Plano de contas vazio para a empresa informada.")
        
        # Processa plano de contas usando AccountMapper
        df_pc = self.account_mapper.processar_plano_contas(df_pc, filtrar_ativas=self.somente_ativas)
        
        # Cria mapas para lookup
        mapas = self.account_mapper.criar_mapas(df_pc)
        self.mapa_clas_to_bc = mapas["clas_to_bc"]
        self.mapa_codi_to_bc = mapas["codi_to_bc"]
        
        self.df_pc = df_pc
        return df_pc
    
    def buscar_saldos_abertura(self) -> pd.DataFrame:
        """
        Busca saldos de abertura (até D-1).
        
        Prefere carregar de CSV cache se fornecido via saldos_path.
        Caso contrário, busca direto do banco de dados.
        
        Returns:
            DataFrame com colunas: BC_ACCOUNT, saldo
        """
        dia_anterior = self.inicio - timedelta(days=1)
        
        if self.saldos_path:
            # Carrega de CSV cache
            saldos_csv = Path(self.saldos_path)
            if not saldos_csv.exists():
                raise FileNotFoundError(f"Arquivo de saldos não encontrado: {saldos_csv}")
            
            df_saldos = pd.read_csv(saldos_csv, sep=";", encoding="utf-8-sig")
            
            # Validações básicas
            if "BC_ACCOUNT" not in df_saldos.columns or "saldo" not in df_saldos.columns:
                raise RuntimeError("CSV de saldos deve conter colunas 'BC_ACCOUNT' e 'saldo'.")
            
            # Opcional: conferir empresa e data_corte, se presentes
            if "empresa" in df_saldos.columns and int(df_saldos["empresa"].iloc[0]) != self.empresa:
                print("[aviso] Empresa do CSV de saldos difere do parâmetro.", file=sys.stderr)
            
            if "data_corte" in df_saldos.columns:
                try:
                    dc = pd.to_datetime(df_saldos["data_corte"].iloc[0]).date()
                    if dc != dia_anterior:
                        print(f"[aviso] data_corte do CSV ({dc}) difere de D-1 ({dia_anterior}).", file=sys.stderr)
                except Exception:
                    pass
            
            # Mantém apenas colunas necessárias
            df_saldos = df_saldos[["BC_ACCOUNT", "saldo"]].copy()
        else:
            # Busca saldos direto do banco até D-1
            df_saldos = self.db_client.buscar_saldos(self.empresa, dia_anterior)
            
            if df_saldos.empty:
                print("[aviso] Nenhum saldo histórico encontrado até D-1. Abertura ficará zerada.", file=sys.stderr)
            
            # Mapeia contas para Beancount (conta é CODI_CTA, não CLAS_CTA)
            df_saldos["conta"] = df_saldos["conta"].astype(str)
            df_saldos["BC_ACCOUNT"] = df_saldos["conta"].map(self.mapa_codi_to_bc)
            df_saldos = df_saldos.dropna(subset=["BC_ACCOUNT"]).copy()
            df_saldos = df_saldos[["BC_ACCOUNT", "saldo"]].copy()
        
        self.df_saldos = df_saldos
        return df_saldos
    
    def buscar_lancamentos(self) -> pd.DataFrame:
        """
        Busca lançamentos contábeis do período.
        
        Returns:
            DataFrame com lançamentos e mapeamento para contas Beancount
        """
        df_lanc = self.db_client.buscar_lancamentos_periodo(self.empresa, self.inicio, self.fim)
        
        if df_lanc.empty:
            print("[aviso] Nenhum lançamento no período informado.", file=sys.stderr)
        
        # Filtra lançamentos de zeramento se desconsiderar_zeramento = True
        if self.desconsiderar_zeramento and "orig_lan" in df_lanc.columns:
            antes = len(df_lanc)
            df_lanc = df_lanc[df_lanc["orig_lan"] != 2].copy()
            depois = len(df_lanc)
            if antes > depois:
                print(
                    f"[info] Excluídos {antes - depois} lançamentos de zeramento (orig_lan = 2).",
                    file=sys.stderr
                )
        
        # Normalizações
        df_lanc["data_lan"] = pd.to_datetime(df_lanc["data_lan"]).dt.date
        df_lanc["vlor_lan"] = pd.to_numeric(df_lanc["vlor_lan"], errors="coerce").fillna(0.0)
        # Mapeia códigos de conta (cdeb_lan e ccre_lan são CODI_CTA) para contas Beancount
        df_lanc["BC_DEB"] = df_lanc["cdeb_lan"].astype(str).map(self.mapa_codi_to_bc)
        df_lanc["BC_CRE"] = df_lanc["ccre_lan"].astype(str).map(self.mapa_codi_to_bc)
        
        self.df_lanc = df_lanc
        return df_lanc
    
    def validar_integridade(self) -> None:
        """
        Valida integridade dos dados:
        - Soma dos saldos de abertura deve ser ≈ 0
        - Lançamentos devem ter partidas dobradas balanceadas
        """
        # Integridade 1: somatório de saldos ~ 0
        if self.df_saldos is not None and not self.df_saldos.empty:
            total_abertura = float(self.df_saldos["saldo"].sum())
            if abs(total_abertura) > 0.01:
                print(
                    f"[alerta] Soma dos saldos de abertura = {total_abertura:.2f} "
                    f"(esperado ~ 0). Verifique contas faltantes no mapa.",
                    file=sys.stderr
                )
        
        # Integridade 2: por nume_lan, débitos == créditos
        if self.df_lanc is not None and not self.df_lanc.empty:
            inconsistentes = []
            for nume, grp in self.df_lanc.groupby("nume_lan"):
                # Conferimos estrutura: nº de linhas deve ser par e contas mapeadas
                if len(grp) % 2 != 0:
                    inconsistentes.append((nume, "linhas_impares"))
                if grp["BC_DEB"].isna().any() or grp["BC_CRE"].isna().any():
                    inconsistentes.append((nume, "conta_sem_mapa"))
                # Soma de débitos == soma de créditos por nume_lan
                soma_debitos = grp["vlor_lan"].sum()
                soma_creditos = grp["vlor_lan"].sum()
                if abs(soma_debitos - soma_creditos) > 0.005:
                    inconsistentes.append((nume, "deb_neq_cred"))
            
            if inconsistentes:
                print(
                    f"[alerta] Lançamentos com potencial inconsistência "
                    f"(nume_lan → motivo): {inconsistentes[:10]} ...",
                    file=sys.stderr
                )
    
    def salvar_mapas_csv(self) -> None:
        """
        Salva arquivos CSV auxiliares:
        - Mapa de classificação → conta Beancount
        - Balancete de abertura
        """
        self.outdir.mkdir(parents=True, exist_ok=True)
        
        # Mapa de contas
        mapa_path = self.outdir / f"mapa_beancount_{self.empresa}.csv"
        if self.df_pc is not None:
            self.df_pc[["CLAS_CTA", "NOME_CTA", "BC_ACCOUNT"]].to_csv(
                mapa_path, index=False, sep=";", encoding="utf-8-sig"
            )
        
        # Balancete de abertura
        dia_anterior = self.inicio - timedelta(days=1)
        bal_abertura_path = self.outdir / f"balancete_abertura_{self.empresa}_{dia_anterior}.csv"
        if self.df_saldos is not None:
            self.df_saldos[["BC_ACCOUNT", "saldo"]].sort_values("BC_ACCOUNT").to_csv(
                bal_abertura_path, index=False, sep=";", encoding="utf-8-sig"
            )
    
    def gerar_beancount(self) -> Path:
        """
        Gera arquivo Beancount formatado.
        
        Returns:
            Caminho do arquivo Beancount gerado
        """
        self.outdir.mkdir(parents=True, exist_ok=True)
        bean_path = self.outdir / f"lancamentos_{self.empresa}_{self.inicio}_{self.fim}.beancount"
        
        # Usa BeancountExporter para gerar o arquivo
        exporter = BeancountExporter(
            df_saldos=self.df_saldos,
            df_lancamentos=self.df_lanc,
            mapa_codi_to_bc=self.mapa_codi_to_bc,
            empresa=self.empresa,
            inicio=self.inicio,
            fim=self.fim,
            moeda=self.moeda,
            abrir_equity_abertura=self.abrir_equity_abertura
        )
        
        return exporter.exportar(bean_path)
    
    def execute(self) -> Path:
        """
        Executa o pipeline completo.
        
        Returns:
            Caminho do arquivo Beancount gerado
        """
        try:
            # Conecta ao banco
            self.db_client.connect()
            
            # Busca plano de contas e mapeia para Beancount
            self.buscar_plano_contas()
            
            # Busca saldos de abertura
            self.buscar_saldos_abertura()
            
            # Busca lançamentos do período
            self.buscar_lancamentos()
            
            # Valida integridade
            self.validar_integridade()
            
            # Salva CSVs auxiliares
            self.salvar_mapas_csv()
            
            # Gera arquivo Beancount
            bean_path = self.gerar_beancount()
            
            return bean_path
            
        finally:
            # Sempre fecha a conexão
            self.db_client.close()


def parse_date(s: str) -> date:
    """Converte string de data para objeto date."""
    try:
        # Aceita ISO (YYYY-MM-DD) e outros formatos
        return isoparse(s).date()
    except Exception:
        # Fallback pandas
        return pd.to_datetime(s, dayfirst=True).date()


def main():
    """Função principal para interface CLI."""
    ap = argparse.ArgumentParser(description="Extrai dados via ODBC e gera arquivo Beancount.")
    ap.add_argument("--dsn", required=False, help="Nome do DSN ODBC", default=None)
    ap.add_argument("--user", required=False, help="Usuário ODBC", default=None)
    ap.add_argument("--password", required=False, help="Senha ODBC", default=None)
    ap.add_argument("--empresa", type=int, required=True, help="CODI_EMP (empresa)")
    ap.add_argument("--inicio", required=True, help="Data inicial (YYYY-MM-DD)")
    ap.add_argument("--fim", required=True, help="Data final (YYYY-MM-DD)")
    ap.add_argument("--moeda", default="BRL", help="Moeda para Beancount (default: BRL)")
    ap.add_argument("--saida", default="./out", help="Diretório de saída (default: ./out)")
    ap.add_argument("--somente-ativas", action="store_true", help="Abrir apenas contas com SITUACAO_CTA = 'A'")
    ap.add_argument("--config", default=None, help="Arquivo INI com [database] dsn/user/password (opcional)")
    ap.add_argument("--saldos", default=None, help="Caminho para CSV de saldos iniciais (cache) gerado por opening_balances.py")
    ap.add_argument("--incluir-zeramento", action="store_true", help="Incluir lançamentos de zeramento (orig_lan = 2). Por padrão, são excluídos.")
    args = ap.parse_args()
    
    inicio = parse_date(args.inicio)
    fim = parse_date(args.fim)
    if fim < inicio:
        raise SystemExit("Data final não pode ser menor que a inicial.")
    
    dsn = args.dsn
    user = args.user
    password = args.password
    classificacao_customizada = None
    
    if args.config:
        cfg = configparser.ConfigParser()
        cfg.read(args.config)
        if not dsn:
            dsn = cfg.get("database", "dsn", fallback=None)
        if not user:
            user = cfg.get("database", "user", fallback=None)
        if not password:
            password = cfg.get("database", "password", fallback=None)
        # Defaults
        args.moeda = cfg.get("defaults", "moeda", fallback=args.moeda)
        if args.empresa is None:
            args.empresa = cfg.getint("defaults", "empresa", fallback=None)
        
        # Carrega classificação customizada se houver
        classifier = AccountClassifier.carregar_do_ini(args.config)
        classificacao_customizada = classifier.mapeamento if classifier else None
    
    if not all([dsn, user, password]):
        raise SystemExit("Informe DSN/USER/PASSWORD via argumentos ou config.ini.")
    
    outdir = Path(args.saida)
    pipeline = BeancountPipeline(
        dsn=dsn,
        user=user,
        password=password,
        empresa=args.empresa,
        inicio=inicio,
        fim=fim,
        moeda=args.moeda,
        outdir=outdir,
        somente_ativas=args.somente_ativas,
        saldos_path=args.saldos,
        classificacao_customizada=classificacao_customizada,
        desconsiderar_zeramento=not args.incluir_zeramento,
    )
    
    try:
        bean_path = pipeline.execute()
        print(f"OK: gerado {bean_path.resolve()}")
    except Exception as e:
        print(f"ERRO: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
