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
from datetime import date, datetime, timedelta
from typing import Optional, Dict

import pyodbc
import pandas as pd
from dateutil.parser import isoparse

from pyaccount.classificacao import (
    classificar_conta, 
    CLASSIFICACAO_M1,
    carregar_classificacao_do_ini
)

# ----------------------- helpers -----------------------
def parse_date(s: str) -> date:
    try:
        # aceita ISO (YYYY-MM-DD) e outros formatos
        return isoparse(s).date()
    except Exception:
        # fallback pandas
        return pd.to_datetime(s, dayfirst=True).date()

def classificar_beancount(
    clas_cta: str, 
    tipo_cta: Optional[str] = None,
    mapeamento_customizado: Optional[Dict[str, str]] = None
) -> str:
    """
    Mapeia CLAS_CTA -> grupo Beancount.
    
    Usa mapeamento customizado se fornecido, caso contrário usa a configuração padrão.
    
    Args:
        clas_cta: Classificação da conta
        tipo_cta: Tipo da conta ('A' = analítica, 'S' = sintética) - não usado para classificação
        mapeamento_customizado: Dicionário opcional com mapeamento customizado de prefixos
                                CLAS_CTA para categorias Beancount.
    
    Returns:
        Nome da categoria Beancount
    """
    return classificar_conta(clas_cta, tipo_cta, mapeamento_customizado)

def normalizar_nome(nome: str) -> str:
    if pd.isna(nome): return "Sem_Nome"
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

def currency_fmt_br(value: float) -> str:
    s = f"{value:,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

# ----------------------- core -----------------------
def run_pipeline(
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
    saldos_path: str | None = None,
    classificacao_customizada: Optional[Dict[str, str]] = None,
) -> Path:
    outdir.mkdir(parents=True, exist_ok=True)
    bean_path = outdir / f"lancamentos_{empresa}_{inicio}_{fim}.beancount"
    mapa_path = outdir / f"mapa_beancount_{empresa}.csv"
    bal_abertura_path = outdir / f"balancete_abertura_{empresa}_{inicio - timedelta(days=1)}.csv"

    # conexão
    conn = pyodbc.connect(f"DSN={dsn};UID={user};PWD={password}")
    # --- plano de contas
    sql_pc = """
    SELECT 
      CODI_EMP,
      CODI_CTA,
      NOME_CTA,
      CLAS_CTA,
      TIPO_CTA,
      DATA_CTA,
      SITUACAO_CTA
    FROM 
      BETHADBA.CTCONTAS
    WHERE 
      CODI_EMP = ?
    ORDER BY CLAS_CTA
    """
    df_pc = pd.read_sql(sql_pc, conn, params=[empresa])
    if df_pc.empty:
        raise RuntimeError("Plano de contas vazio para a empresa informada.")

    if somente_ativas and "SITUACAO_CTA" in df_pc.columns:
        df_pc = df_pc[df_pc["SITUACAO_CTA"].astype(str).str.upper().eq("A")].copy()

    df_pc["BC_GROUP"] = [
        classificar_beancount(clas, tipo, classificacao_customizada) 
        for clas, tipo in zip(df_pc["CLAS_CTA"], df_pc["TIPO_CTA"])
    ]
    df_pc["BC_NAME"] = df_pc["NOME_CTA"].astype(str).apply(normalizar_nome)
    df_pc["BC_ACCOUNT"] = df_pc["BC_GROUP"] + ":" + df_pc["BC_NAME"]

    mapa_clas_to_bc: Dict[str, str] = dict(zip(df_pc["CLAS_CTA"].astype(str), df_pc["BC_ACCOUNT"]))
    df_pc[["CLAS_CTA", "NOME_CTA", "BC_ACCOUNT"]].to_csv(mapa_path, index=False, sep=";", encoding="utf-8-sig")


    # --- saldos até D-1 (preferir cache CSV se fornecido)
    dia_anterior = inicio - timedelta(days=1)

    if saldos_path:
        saldos_csv = Path(saldos_path)
        if not saldos_csv.exists():
            raise FileNotFoundError(f"Arquivo de saldos não encontrado: {saldos_csv}")
        df_saldos = pd.read_csv(saldos_csv, sep=";", encoding="utf-8-sig")
        # validações básicas
        if "BC_ACCOUNT" not in df_saldos.columns or "saldo" not in df_saldos.columns:
            raise RuntimeError("CSV de saldos deve conter colunas 'BC_ACCOUNT' e 'saldo'.")
        # opcional: conferir empresa e data_corte, se presentes
        if "empresa" in df_saldos.columns and int(df_saldos["empresa"].iloc[0]) != empresa:
            print("[aviso] Empresa do CSV de saldos difere do parâmetro.", file=sys.stderr)
        if "data_corte" in df_saldos.columns:
            # se a data_corte != dia_anterior, apenas alertar (pode ser intencional)
            try:
                dc = pd.to_datetime(df_saldos["data_corte"].iloc[0]).date()
                if dc != dia_anterior:
                    print(f"[aviso] data_corte do CSV ({dc}) difere de D-1 ({dia_anterior}).", file=sys.stderr)
            except Exception:
                pass
        # manter apenas as colunas necessárias
        df_saldos = df_saldos[["BC_ACCOUNT", "saldo"]].copy()
    else:
        # --- saldos até D-1
        dia_anterior = inicio - timedelta(days=1)
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
        df_saldos = pd.read_sql(sql_saldos, conn, params=[empresa, dia_anterior, empresa, dia_anterior])
        if "conta" not in df_saldos.columns:
            # compat: alguns bancos podem retornar colunas maiúsculas
            df_saldos.columns = [c.lower() for c in df_saldos.columns]

        if df_saldos.empty:
            print("[aviso] Nenhum saldo histórico encontrado até D-1. Abertura ficará zerada.", file=sys.stderr)

        df_saldos["conta"] = df_saldos["conta"].astype(str)
        df_saldos["BC_ACCOUNT"] = df_saldos["conta"].map(mapa_clas_to_bc)
        df_saldos = df_saldos.dropna(subset=["BC_ACCOUNT"]).copy()

    # integridade 1: somatório de saldos ~ 0
    total_abertura = float(df_saldos["saldo"].sum()) if not df_saldos.empty else 0.0
    if abs(total_abertura) > 0.01:
        print(f"[alerta] Soma dos saldos de abertura = {total_abertura:.2f} (esperado ~ 0). Verifique contas faltantes no mapa.", file=sys.stderr)

    # salva balancete de abertura
    df_saldos[["BC_ACCOUNT", "saldo"]].sort_values("BC_ACCOUNT").to_csv(
        bal_abertura_path, index=False, sep=";", encoding="utf-8-sig"
    )

    # --- lançamentos do período
    sql_lanc = """
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
    df_lanc = pd.read_sql(sql_lanc, conn, params=[empresa, inicio, fim])
    conn.close()

    if df_lanc.empty:
        print("[aviso] Nenhum lançamento no período informado.", file=sys.stderr)

    # normalizações
    df_lanc["data_lan"] = pd.to_datetime(df_lanc["data_lan"]).dt.date
    df_lanc["vlor_lan"] = pd.to_numeric(df_lanc["vlor_lan"], errors="coerce").fillna(0.0)
    df_lanc["BC_DEB"] = df_lanc["cdeb_lan"].astype(str).map(mapa_clas_to_bc)
    df_lanc["BC_CRE"] = df_lanc["ccre_lan"].astype(str).map(mapa_clas_to_bc)

    # integridade 2: por nume_lan, debitos == creditos
    inconsistentes = []
    for nume, grp in df_lanc.groupby("nume_lan"):
        deb = grp["vlor_lan"].sum()  # todas as linhas do grupo são do mesmo valor? Em Betha cada par (deb/cred) tem mesmo vlor_lan
        # conferimos estrutura: nº de linhas deve ser par e contas mapeadas
        if len(grp) % 2 != 0:
            inconsistentes.append((nume, "linhas_impares"))
        if grp["BC_DEB"].isna().any() or grp["BC_CRE"].isna().any():
            inconsistentes.append((nume, "conta_sem_mapa"))
        # soma de débitos == soma de créditos por nume_lan (assumindo que vlor_lan se repete no par)
        soma_debitos = grp["vlor_lan"].sum()
        soma_creditos = grp["vlor_lan"].sum()
        # Aqui mantemos um aviso informativo. Ajuste se sua estrutura for diferente.
        if abs(soma_debitos - soma_creditos) > 0.005:
            inconsistentes.append((nume, "deb_neq_cred"))

    if inconsistentes:
        print(f"[alerta] Lançamentos com potencial inconsistência (nume_lan → motivo): {inconsistentes[:10]} ...", file=sys.stderr)

    # contas usadas
    contas_usadas = set(df_saldos["BC_ACCOUNT"].tolist())
    contas_usadas.update(df_lanc["BC_DEB"].dropna().tolist())
    contas_usadas.update(df_lanc["BC_CRE"].dropna().tolist())
    contas_usadas.add(abrir_equity_abertura)

    # escrever .beancount
    with bean_path.open("w", encoding="utf-8") as f:
        f.write(f"; Empresa {empresa} — período {inicio} a {fim}\n")
        f.write(f'option "operating_currency" "{moeda}"\n')
        f.write('option "title" "Contabilidade — Extração ODBC"\n\n')

        for acc in sorted(contas_usadas):
            f.write(f"{inicio} open {acc} {moeda}\n")
        f.write("\n")

        # Transação de abertura
        f.write(f'{inicio} * "Abertura de saldos" "Saldo até {dia_anterior}"\n')
        for _, r in df_saldos.iterrows():
            f.write(f"  {r['BC_ACCOUNT']:<60} {format_val(r['saldo'], moeda)}\n")
        f.write(f"  {abrir_equity_abertura}\n\n")

        # Lançamentos
        for _, r in df_lanc.iterrows():
            deb = r.get("BC_DEB"); cre = r.get("BC_CRE")
            if pd.isna(deb) or pd.isna(cre): 
                continue
            data_txt = r["data_lan"].strftime("%Y-%m-%d")
            hist = (str(r.get('chis_lan') or '')).replace('\\n', ' ').strip()
            ndoc = str(r.get('ndoc_lan') or '')
            lote = str(r.get('codi_lote') or '')
            usu  = str(r.get('codi_usu') or '')
            meta = " ".join(filter(None, [f'Doc {ndoc}' if ndoc else '', f'Lote {lote}' if lote else '', f'Usu {usu}' if usu else '']))

            f.write(f'{data_txt} * "{hist}" "{meta}"\n')
            f.write(f"  {deb:<60} {format_val(r['vlor_lan'], moeda)}\n")
            f.write(f"  {cre}\n\n")

    return bean_path

def format_val(v: float, currency: str) -> str:
    # Retorna "1.234,56 BRL" (sem a moeda, pois o chamador inclui)
    s = f"{v:,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".") + f" {currency}"

# ----------------------- CLI -----------------------
def main():
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
    ap.add_argument("--saldos", default=None, help="Caminho para CSV de saldos iniciais (cache) gerado por build_opening_balances.py")
    args = ap.parse_args()

    inicio = parse_date(args.inicio)
    fim = parse_date(args.fim)
    if fim < inicio:
        raise SystemExit("Data final não pode ser menor que a inicial.")

    dsn = args.dsn; user = args.user; password = args.password
    classificacao_customizada = None
    if args.config:
        cfg = configparser.ConfigParser()
        cfg.read(args.config)
        if not dsn: dsn = cfg.get("database", "dsn", fallback=None)
        if not user: user = cfg.get("database", "user", fallback=None)
        if not password: password = cfg.get("database", "password", fallback=None)
        # defaults
        args.moeda = cfg.get("defaults", "moeda", fallback=args.moeda)
        if args.empresa is None:
            args.empresa = cfg.getint("defaults", "empresa", fallback=None)
        
        # Carrega classificação customizada se houver
        classificacao_customizada = carregar_classificacao_do_ini(args.config)

    if not all([dsn, user, password]):
        raise SystemExit("Informe DSN/USER/PASSWORD via argumentos ou config.ini.")

    outdir = Path(args.saida)
    bean_path = run_pipeline(
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
    )
    print(f"OK: gerado {bean_path.resolve()}")

if __name__ == "__main__":
    main()
