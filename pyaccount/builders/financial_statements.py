#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo para geração de demonstrações financeiras.

Classes base para construção de:
- Balanço Patrimonial
- Balancete
- DRE (Demonstração do Resultado do Exercício)
- Extrato/Movimentação do Período
"""
from typing import Optional, List, Dict
import sys
import pandas as pd

from pyaccount.core.account_mapper import AccountMapper


class _FinancialStatementBase:
    """
    Classe base com métodos auxiliares comuns para construção de demonstrações financeiras.
    """
    
    @staticmethod
    def _merge_com_plano_contas(
        df: pd.DataFrame,
        df_plano_contas: pd.DataFrame,
        coluna_conta_df: str = "conta",
        coluna_conta_pc: str = "CODI_CTA",
        colunas_pc: Optional[list] = None
    ) -> pd.DataFrame:
        """
        Mescla DataFrame com plano de contas, convertendo tipos para garantir compatibilidade.
        
        Args:
            df: DataFrame a ser mesclado (deve ter coluna de conta)
            df_plano_contas: DataFrame com plano de contas
            coluna_conta_df: Nome da coluna de conta no DataFrame principal
            coluna_conta_pc: Nome da coluna de conta no plano de contas
            colunas_pc: Lista de colunas do plano de contas a incluir (None = todas)
            
        Returns:
            DataFrame mesclado
        """
        df_result = df.copy()
        df_pc = df_plano_contas.copy()
        
        # Converte para string e remove espaços
        df_result[coluna_conta_df] = df_result[coluna_conta_df].astype(str).str.strip()
        df_pc[coluna_conta_pc] = df_pc[coluna_conta_pc].astype(str).str.strip()
        
        # Seleciona colunas do plano de contas
        if colunas_pc is None:
            colunas_pc = df_pc.columns.tolist()
        
        # Filtra apenas colunas que existem no DataFrame
        colunas_pc_disponiveis = [c for c in colunas_pc if c in df_pc.columns]
        
        # Cria coluna temporária para merge se necessário
        coluna_temp = f"{coluna_conta_pc}_str"
        if coluna_temp not in df_pc.columns:
            df_pc[coluna_temp] = df_pc[coluna_conta_pc]
        
        # Mescla
        colunas_merge = [coluna_temp] + [c for c in colunas_pc_disponiveis if c != coluna_conta_pc and c != coluna_temp]
        df_result = df_result.merge(
            df_pc[colunas_merge],
            left_on=coluna_conta_df,
            right_on=coluna_temp,
            how="left"
        )
        
        # Remove coluna temporária
        df_result = df_result.drop(columns=[coluna_temp], errors="ignore")
        
        return df_result
    
    @staticmethod
    def _preencher_e_classificar(
        df: pd.DataFrame,
        account_mapper: AccountMapper,
        preencher_nome: str = "Conta não encontrada",
        preencher_clas: str = ""
    ) -> pd.DataFrame:
        """
        Preenche valores faltantes e classifica contas sem BC_GROUP.
        
        Args:
            df: DataFrame a ser processado
            account_mapper: Instância de AccountMapper para classificação
            preencher_nome: Valor padrão para NOME_CTA faltante
            preencher_clas: Valor padrão para CLAS_CTA faltante
            
        Returns:
            DataFrame processado
        """
        # Preenche valores faltantes
        if "NOME_CTA" in df.columns:
            df["NOME_CTA"] = df["NOME_CTA"].fillna(preencher_nome).astype(str)
        if "CLAS_CTA" in df.columns:
            df["CLAS_CTA"] = df["CLAS_CTA"].fillna(preencher_clas).astype(str)
        
        # Cria ou atualiza coluna BC_GROUP apenas se não existir ou estiver vazia
        if "BC_GROUP" not in df.columns:
            df["BC_GROUP"] = None
        
        # Classifica apenas contas sem BC_GROUP (se já foi calculado durante importação, mantém)
        mask_sem_bc_group = df["BC_GROUP"].isna() | (df["BC_GROUP"] == "") | (df["BC_GROUP"].astype(str).str.strip() == "")
        if mask_sem_bc_group.any():
            df.loc[mask_sem_bc_group, "BC_GROUP"] = df.loc[mask_sem_bc_group].apply(
                lambda row: account_mapper.classificar_beancount(
                    str(row.get("CLAS_CTA", "") or ""), 
                    str(row.get("TIPO_CTA", ""))
                ),
                axis=1
            )
        
        df["BC_GROUP"] = df["BC_GROUP"].fillna("Unknown").astype(str)
        
        return df


class BalanceSheetBuilder:
    """
    Construtor de Balanço Patrimonial.
    
    Gera estrutura do Balanço Patrimonial agrupando contas por categoria Beancount.
    """
    
    def __init__(
        self,
        df_saldos_finais: pd.DataFrame,
        df_plano_contas: pd.DataFrame,
        account_mapper: AccountMapper
    ):
        """
        Inicializa o construtor de Balanço Patrimonial.
        
        Args:
            df_saldos_finais: DataFrame com saldos finais (deve ter colunas: conta, saldo)
            df_plano_contas: DataFrame com plano de contas (deve ter colunas: CODI_CTA, CLAS_CTA, NOME_CTA, BC_GROUP)
            account_mapper: Instância de AccountMapper para classificação
        """
        self.df_saldos_finais = df_saldos_finais
        self.df_plano_contas = df_plano_contas
        self.account_mapper = account_mapper
    
    def gerar(self) -> pd.DataFrame:
        """
        Gera estrutura do Balanço Patrimonial.
        
        Returns:
            DataFrame com colunas: Conta/Categoria, Saldo
        """
        if self.df_saldos_finais is None or self.df_saldos_finais.empty:
            return pd.DataFrame()
        
        # Mescla saldos com plano de contas
        df_bp = _FinancialStatementBase._merge_com_plano_contas(
            self.df_saldos_finais,
            self.df_plano_contas,
            coluna_conta_df="conta",
            coluna_conta_pc="CODI_CTA",
            colunas_pc=["CODI_CTA", "CLAS_CTA", "NOME_CTA", "BC_GROUP"]
        )
        
        # Preenche e classifica
        df_bp = _FinancialStatementBase._preencher_e_classificar(
            df_bp,
            self.account_mapper
        )
        
        # Agrupa por categoria Beancount
        linhas_bp = []
        
        # Assets (Ativo)
        assets = df_bp[df_bp["BC_GROUP"].str.startswith("Assets", na=False)].copy()
        if not assets.empty:
            linhas_bp.append({"Conta/Categoria": "ATIVO", "Saldo": None})
            
            # Ativo Circulante
            ativo_circ = assets[assets["BC_GROUP"].str.contains("Ativo-Circulante", na=False)]
            if not ativo_circ.empty:
                total_circ = ativo_circ["saldo"].sum()
                linhas_bp.append({"Conta/Categoria": "  Ativo Circulante", "Saldo": total_circ})
                for _, row in ativo_circ.iterrows():
                    linhas_bp.append({
                        "Conta/Categoria": f"    {row['NOME_CTA']} ({row['conta']})",
                        "Saldo": row["saldo"]
                    })
            
            # Ativo Não Circulante
            ativo_ncirc = assets[assets["BC_GROUP"].str.contains("Ativo-Nao-Circulante", na=False)]
            if not ativo_ncirc.empty:
                total_ncirc = ativo_ncirc["saldo"].sum()
                linhas_bp.append({"Conta/Categoria": "  Ativo Não Circulante", "Saldo": total_ncirc})
                for _, row in ativo_ncirc.iterrows():
                    linhas_bp.append({
                        "Conta/Categoria": f"    {row['NOME_CTA']} ({row['conta']})",
                        "Saldo": row["saldo"]
                    })
            
            total_ativo = assets["saldo"].sum()
            linhas_bp.append({"Conta/Categoria": "TOTAL ATIVO", "Saldo": total_ativo})
            linhas_bp.append({"Conta/Categoria": "", "Saldo": None})
        
        # Liabilities (Passivo)
        liabilities = df_bp[df_bp["BC_GROUP"].str.startswith("Liabilities", na=False)].copy()
        if not liabilities.empty:
            linhas_bp.append({"Conta/Categoria": "PASSIVO", "Saldo": None})
            
            # Passivo Circulante
            passivo_circ = liabilities[liabilities["BC_GROUP"].str.contains("Passivo-Circulante", na=False)]
            if not passivo_circ.empty:
                total_circ = passivo_circ["saldo"].sum()
                linhas_bp.append({"Conta/Categoria": "  Passivo Circulante", "Saldo": total_circ})
                for _, row in passivo_circ.iterrows():
                    linhas_bp.append({
                        "Conta/Categoria": f"    {row['NOME_CTA']} ({row['conta']})",
                        "Saldo": row["saldo"]
                    })
            
            # Passivo Não Circulante
            passivo_ncirc = liabilities[liabilities["BC_GROUP"].str.contains("Passivo-Nao-Circulante", na=False)]
            if not passivo_ncirc.empty:
                total_ncirc = passivo_ncirc["saldo"].sum()
                linhas_bp.append({"Conta/Categoria": "  Passivo Não Circulante", "Saldo": total_ncirc})
                for _, row in passivo_ncirc.iterrows():
                    linhas_bp.append({
                        "Conta/Categoria": f"    {row['NOME_CTA']} ({row['conta']})",
                        "Saldo": row["saldo"]
                    })
            
            total_passivo = liabilities["saldo"].sum()
            linhas_bp.append({"Conta/Categoria": "TOTAL PASSIVO", "Saldo": total_passivo})
            linhas_bp.append({"Conta/Categoria": "", "Saldo": None})
        
        # Equity (Patrimônio Líquido)
        equity = df_bp[df_bp["BC_GROUP"].str.startswith("Equity", na=False)].copy()
        if not equity.empty:
            linhas_bp.append({"Conta/Categoria": "PATRIMÔNIO LÍQUIDO", "Saldo": None})
            
            pl_contas = equity[~equity["BC_GROUP"].str.contains("Contas-", na=False)]
            if not pl_contas.empty:
                for _, row in pl_contas.iterrows():
                    linhas_bp.append({
                        "Conta/Categoria": f"  {row['NOME_CTA']} ({row['conta']})",
                        "Saldo": row["saldo"]
                    })
            
            total_pl = equity["saldo"].sum()
            linhas_bp.append({"Conta/Categoria": "TOTAL PATRIMÔNIO LÍQUIDO", "Saldo": total_pl})
            linhas_bp.append({"Conta/Categoria": "", "Saldo": None})
            
            total_geral = (assets["saldo"].sum() if not assets.empty else 0) + \
                         (liabilities["saldo"].sum() if not liabilities.empty else 0) + \
                         total_pl
            linhas_bp.append({"Conta/Categoria": "TOTAL GERAL", "Saldo": total_geral})
        
        return pd.DataFrame(linhas_bp)


class IncomeStatementBuilder:
    """
    Construtor de DRE (Demonstração do Resultado do Exercício).
    
    Gera estrutura da DRE agrupando receitas e despesas por categoria.
    """
    
    def __init__(
        self,
        df_movimentacoes: pd.DataFrame,
        df_plano_contas: pd.DataFrame,
        account_mapper: AccountMapper,
        agrupamento_periodo: Optional[str] = None
    ):
        """
        Inicializa o construtor de DRE.
        
        Args:
            df_movimentacoes: DataFrame com movimentações do período.
                             Se agrupamento_periodo for None: deve ter colunas (conta, movimento).
                             Se agrupamento_periodo for "anual", "mensal" ou "trimestral": deve ter colunas (conta, periodo, movimento).
            df_plano_contas: DataFrame com plano de contas (deve ter colunas: CODI_CTA, CLAS_CTA, NOME_CTA, BC_GROUP)
            account_mapper: Instância de AccountMapper para classificação
            agrupamento_periodo: Tipo de agrupamento por período. Valores aceitos:
                                None (sem agrupamento, campo TOTAL PERIODO), "anual" (agrupa por anos como 2024, 2025),
                                "mensal" (agrupa por meses como Jan/24, Fev/24) ou "trimestral" (agrupa por trimestres como 1T/24, 2T/24)
        """
        self.df_movimentacoes = df_movimentacoes
        self.df_plano_contas = df_plano_contas
        self.account_mapper = account_mapper
        self.agrupamento_periodo = agrupamento_periodo
    
    def gerar(self) -> pd.DataFrame:
        """
        Gera estrutura da DRE.
        
        Returns:
            Se agrupamento_periodo for None: DataFrame com colunas (Item, TOTAL PERIODO)
            Se agrupamento_periodo for "anual": DataFrame com colunas (Item + colunas dinâmicas por ano + Total)
            Se agrupamento_periodo for "mensal" ou "trimestral": DataFrame com colunas (Item + colunas dinâmicas por período + Total)
        """
        if self.df_movimentacoes is None or self.df_movimentacoes.empty:
            return pd.DataFrame()
        
        # Se há agrupamento por período (anual, mensal ou trimestral), usa método específico
        if self.agrupamento_periodo in ["anual", "mensal", "trimestral"]:
            return self._processar_dre_por_periodo()
        
        # Se agrupamento_periodo é None, trata como sem agrupamento (campo TOTAL PERIODO)
        return self._processar_dre_anual()
    
    def _processar_dre_anual(self) -> pd.DataFrame:
        """
        Processa DRE sem agrupamento por período (agrupamento_periodo=None).
        
        Returns:
            DataFrame com colunas (Item, TOTAL PERIODO)
        """
        # Mescla movimentações com plano de contas
        df_dre = _FinancialStatementBase._merge_com_plano_contas(
            self.df_movimentacoes,
            self.df_plano_contas,
            coluna_conta_df="conta",
            coluna_conta_pc="CODI_CTA",
            colunas_pc=["CODI_CTA", "CLAS_CTA", "NOME_CTA", "BC_GROUP"]
        )
        
        # Preenche e classifica
        df_dre = _FinancialStatementBase._preencher_e_classificar(
            df_dre,
            self.account_mapper
        )
        
        # Debug: alerta sobre contas classificadas como Unknown
        self._debug_unknown_accounts(df_dre)
        
        linhas_dre = []
        
        # Income (Receitas) - mostra todas as receitas
        # Receitas são creditadas (movimento negativo), mas na DRE devem aparecer POSITIVAS
        income = df_dre[df_dre["BC_GROUP"].str.startswith("Income", na=False)].copy()
        if not income.empty:
            # Inverte sinal das receitas (de negativo para positivo)
            income["movimento"] = -income["movimento"]
            
            linhas_dre.append({"Item": "RECEITAS", "Valor": None})
            
            # Receitas Operacionais
            rec_op = income[income["BC_GROUP"].str.contains("Operacionais", na=False)]
            if not rec_op.empty:
                total_rec_op = rec_op["movimento"].sum()
                linhas_dre.append({"Item": "  Receitas Operacionais", "Valor": total_rec_op})
                for _, row in rec_op.iterrows():
                    nome_cta = str(row.get("NOME_CTA", "Conta desconhecida"))
                    conta = str(row.get("conta", ""))
                    linhas_dre.append({
                        "Item": f"    {nome_cta} ({conta})",
                        "Valor": row["movimento"]
                    })
                linhas_dre.append({"Item": "  Total Receitas Operacionais", "Valor": total_rec_op})
                linhas_dre.append({"Item": "", "Valor": None})
            
            # Receitas Financeiras
            rec_fin = income[income["BC_GROUP"].str.contains("Financeiras", na=False)]
            if not rec_fin.empty:
                total_rec_fin = rec_fin["movimento"].sum()
                linhas_dre.append({"Item": "  Receitas Financeiras", "Valor": total_rec_fin})
                for _, row in rec_fin.iterrows():
                    nome_cta = str(row.get("NOME_CTA", "Conta desconhecida"))
                    conta = str(row.get("conta", ""))
                    linhas_dre.append({
                        "Item": f"    {nome_cta} ({conta})",
                        "Valor": row["movimento"]
                    })
                linhas_dre.append({"Item": "  Total Receitas Financeiras", "Valor": total_rec_fin})
                linhas_dre.append({"Item": "", "Valor": None})
            
            # Outras Receitas
            outras_rec = income[
                ~income["BC_GROUP"].str.contains("Operacionais", na=False) &
                ~income["BC_GROUP"].str.contains("Financeiras", na=False)
            ]
            if not outras_rec.empty:
                total_outras_rec = outras_rec["movimento"].sum()
                linhas_dre.append({"Item": "  Outras Receitas", "Valor": total_outras_rec})
                for _, row in outras_rec.iterrows():
                    nome_cta = str(row.get("NOME_CTA", "Conta desconhecida"))
                    conta = str(row.get("conta", ""))
                    linhas_dre.append({
                        "Item": f"    {nome_cta} ({conta})",
                        "Valor": row["movimento"]
                    })
                linhas_dre.append({"Item": "  Total Outras Receitas", "Valor": total_outras_rec})
                linhas_dre.append({"Item": "", "Valor": None})
            
            total_receitas = income["movimento"].sum()
            linhas_dre.append({"Item": "TOTAL RECEITAS", "Valor": total_receitas})
            linhas_dre.append({"Item": "", "Valor": None})
        
        # Expenses (Custos e Despesas) - mostra todas as despesas
        # Despesas são debitadas (movimento positivo), mas na DRE devem aparecer NEGATIVAS
        expenses = df_dre[df_dre["BC_GROUP"].str.startswith("Expenses", na=False)].copy()
        if not expenses.empty:
            # Inverte sinal das despesas (de positivo para negativo)
            expenses["movimento"] = -expenses["movimento"]
            
            linhas_dre.append({"Item": "(-) CUSTOS E DESPESAS", "Valor": None})
            
            # Custos
            custos = expenses[expenses["BC_GROUP"].str.contains("Custos", na=False)]
            if not custos.empty:
                total_custos = custos["movimento"].sum()
                linhas_dre.append({"Item": "  (-) Custos", "Valor": total_custos})
                for _, row in custos.iterrows():
                    nome_cta = str(row.get("NOME_CTA", "Conta desconhecida"))
                    conta = str(row.get("conta", ""))
                    linhas_dre.append({
                        "Item": f"    {nome_cta} ({conta})",
                        "Valor": row["movimento"]
                    })
                linhas_dre.append({"Item": "  Total Custos", "Valor": total_custos})
                linhas_dre.append({"Item": "", "Valor": None})
            
            # Despesas Operacionais
            desp_op = expenses[expenses["BC_GROUP"].str.contains("Despesas-Operacionais", na=False)]
            if not desp_op.empty:
                total_desp_op = desp_op["movimento"].sum()
                linhas_dre.append({"Item": "  (-) Despesas Operacionais", "Valor": total_desp_op})
                for _, row in desp_op.iterrows():
                    nome_cta = str(row.get("NOME_CTA", "Conta desconhecida"))
                    conta = str(row.get("conta", ""))
                    linhas_dre.append({
                        "Item": f"    {nome_cta} ({conta})",
                        "Valor": row["movimento"]
                    })
                linhas_dre.append({"Item": "  Total Despesas Operacionais", "Valor": total_desp_op})
                linhas_dre.append({"Item": "", "Valor": None})
            
            # Despesas Financeiras
            desp_fin = expenses[expenses["BC_GROUP"].str.contains("Despesas-Financeiras", na=False)]
            if not desp_fin.empty:
                total_desp_fin = desp_fin["movimento"].sum()
                linhas_dre.append({"Item": "  (-) Despesas Financeiras", "Valor": total_desp_fin})
                for _, row in desp_fin.iterrows():
                    nome_cta = str(row.get("NOME_CTA", "Conta desconhecida"))
                    conta = str(row.get("conta", ""))
                    linhas_dre.append({
                        "Item": f"    {nome_cta} ({conta})",
                        "Valor": row["movimento"]
                    })
                linhas_dre.append({"Item": "  Total Despesas Financeiras", "Valor": total_desp_fin})
                linhas_dre.append({"Item": "", "Valor": None})
            
            # Outras Despesas
            outras_desp = expenses[
                ~expenses["BC_GROUP"].str.contains("Custos", na=False) &
                ~expenses["BC_GROUP"].str.contains("Despesas-Operacionais", na=False) &
                ~expenses["BC_GROUP"].str.contains("Despesas-Financeiras", na=False)
            ]
            if not outras_desp.empty:
                total_outras_desp = outras_desp["movimento"].sum()
                linhas_dre.append({"Item": "  (-) Outras Despesas", "Valor": total_outras_desp})
                for _, row in outras_desp.iterrows():
                    nome_cta = str(row.get("NOME_CTA", "Conta desconhecida"))
                    conta = str(row.get("conta", ""))
                    linhas_dre.append({
                        "Item": f"    {nome_cta} ({conta})",
                        "Valor": row["movimento"]
                    })
                linhas_dre.append({"Item": "  Total Outras Despesas", "Valor": total_outras_desp})
                linhas_dre.append({"Item": "", "Valor": None})
            
            total_despesas = expenses["movimento"].sum()
            linhas_dre.append({"Item": "TOTAL DESPESAS", "Valor": total_despesas})
            linhas_dre.append({"Item": "", "Valor": None})
        
        # Resultado
        total_receitas_val = income["movimento"].sum() if not income.empty else 0
        total_despesas_val = expenses["movimento"].sum() if not expenses.empty else 0
        resultado = total_receitas_val + total_despesas_val  # Despesas já são negativas
        
        linhas_dre.append({"Item": "RESULTADO DO PERÍODO", "Valor": resultado})
        
        return pd.DataFrame(linhas_dre)
    
    def _processar_dre_por_periodo(self) -> pd.DataFrame:
        """
        Processa DRE com agrupamento por período.
        
        Returns:
            DataFrame com colunas: Item + colunas dinâmicas por período + Total
        """
        # Mescla movimentações com plano de contas
        df_dre = _FinancialStatementBase._merge_com_plano_contas(
            self.df_movimentacoes,
            self.df_plano_contas,
            coluna_conta_df="conta",
            coluna_conta_pc="CODI_CTA",
            colunas_pc=["CODI_CTA", "CLAS_CTA", "NOME_CTA", "BC_GROUP"]
        )
        
        # Preenche e classifica
        df_dre = _FinancialStatementBase._preencher_e_classificar(
            df_dre,
            self.account_mapper
        )
        
        # Debug: alerta sobre contas classificadas como Unknown
        self._debug_unknown_accounts(df_dre)
        
        # Obtém lista ordenada de períodos (cronologicamente)
        periodos_unicos = df_dre["periodo"].unique().tolist()
        
        # Ordena períodos cronologicamente
        if self.agrupamento_periodo == "anual":
            # Ordena por ano numericamente: "2024", "2025", etc.
            periodos_ordenados = sorted(
                periodos_unicos,
                key=lambda p: int(p) if p.isdigit() else 0
            )
            periodos = periodos_ordenados
        elif self.agrupamento_periodo == "mensal":
            # Ordena por data: converte "Jan/24" para datetime e ordena
            periodos_com_data = []
            for p in periodos_unicos:
                try:
                    # Tenta parsear como "Jan/24" -> adiciona "/01" para ter dia completo
                    dt = pd.to_datetime(f"01/{p}", format="%d/%b/%y", errors="coerce")
                    if pd.notna(dt):
                        periodos_com_data.append((dt, p))
                except:
                    pass
            # Ordena por data e extrai os períodos
            periodos_com_data.sort(key=lambda x: x[0])
            periodos = [p for _, p in periodos_com_data]
            # Adiciona períodos que não foram parseados (mantém ordem original)
            for p in periodos_unicos:
                if p not in periodos:
                    periodos.append(p)
        elif self.agrupamento_periodo == "trimestral":
            # Ordena por ano e trimestre: "1T/24" -> (ano=24, trimestre=1)
            periodos_ordenados = sorted(
                periodos_unicos,
                key=lambda p: (
                    int(p.split("/")[1]) if "/" in p else 0,  # Ano
                    int(p.split("T/")[0]) if "T/" in p else 0  # Trimestre
                ) if "/" in p and "T/" in p else (0, 0)
            )
            periodos = periodos_ordenados
        else:
            # Fallback: ordenação alfabética
            periodos = sorted(periodos_unicos)
        
        # Cria pivot table: contas x períodos
        df_pivot = df_dre.pivot_table(
            index=["conta", "NOME_CTA", "BC_GROUP"],
            columns="periodo",
            values="movimento",
            aggfunc="sum",
            fill_value=0.0
        ).reset_index()
        
        # Calcula total por conta
        df_pivot["Total"] = df_pivot[periodos].sum(axis=1)
        
        # Remove contas com total zero
        df_pivot = df_pivot[df_pivot["Total"] != 0].copy()
        
        linhas_dre = []
        
        # Income (Receitas) - mostra todas as receitas
        # Receitas são creditadas (movimento negativo), mas na DRE devem aparecer POSITIVAS
        income = df_pivot[df_pivot["BC_GROUP"].str.startswith("Income", na=False)].copy()
        if not income.empty:
            # Inverte sinal das receitas (de negativo para positivo)
            for periodo in periodos:
                income[periodo] = -income[periodo]
            income["Total"] = -income["Total"]
            
            linhas_dre.append(self._criar_linha_titulo("RECEITAS", periodos))
            
            # Receitas Operacionais
            rec_op = income[income["BC_GROUP"].str.contains("Operacionais", na=False)]
            if not rec_op.empty:
                total_rec_op = rec_op["Total"].sum()
                linhas_dre.append(self._criar_linha_subtotal("  Receitas Operacionais", rec_op, periodos))
                for _, row in rec_op.iterrows():
                    nome_cta = str(row.get("NOME_CTA", "Conta desconhecida"))
                    conta = str(row.get("conta", ""))
                    linhas_dre.append(self._criar_linha_conta(f"    {nome_cta} ({conta})", row, periodos))
                linhas_dre.append(self._criar_linha_subtotal("  Total Receitas Operacionais", rec_op, periodos))
                linhas_dre.append(self._criar_linha_vazia(periodos))
            
            # Receitas Financeiras
            rec_fin = income[income["BC_GROUP"].str.contains("Financeiras", na=False)]
            if not rec_fin.empty:
                linhas_dre.append(self._criar_linha_subtotal("  Receitas Financeiras", rec_fin, periodos))
                for _, row in rec_fin.iterrows():
                    nome_cta = str(row.get("NOME_CTA", "Conta desconhecida"))
                    conta = str(row.get("conta", ""))
                    linhas_dre.append(self._criar_linha_conta(f"    {nome_cta} ({conta})", row, periodos))
                linhas_dre.append(self._criar_linha_subtotal("  Total Receitas Financeiras", rec_fin, periodos))
                linhas_dre.append(self._criar_linha_vazia(periodos))
            
            # Outras Receitas
            outras_rec = income[
                ~income["BC_GROUP"].str.contains("Operacionais", na=False) &
                ~income["BC_GROUP"].str.contains("Financeiras", na=False)
            ]
            if not outras_rec.empty:
                linhas_dre.append(self._criar_linha_subtotal("  Outras Receitas", outras_rec, periodos))
                for _, row in outras_rec.iterrows():
                    nome_cta = str(row.get("NOME_CTA", "Conta desconhecida"))
                    conta = str(row.get("conta", ""))
                    linhas_dre.append(self._criar_linha_conta(f"    {nome_cta} ({conta})", row, periodos))
                linhas_dre.append(self._criar_linha_subtotal("  Total Outras Receitas", outras_rec, periodos))
                linhas_dre.append(self._criar_linha_vazia(periodos))
            
            total_receitas = income["Total"].sum()
            linhas_dre.append(self._criar_linha_total("TOTAL RECEITAS", income, periodos))
            linhas_dre.append(self._criar_linha_vazia(periodos))
        
        # Expenses (Custos e Despesas) - mostra todas as despesas
        # Despesas são debitadas (movimento positivo), mas na DRE devem aparecer NEGATIVAS
        expenses = df_pivot[df_pivot["BC_GROUP"].str.startswith("Expenses", na=False)].copy()
        if not expenses.empty:
            # Inverte sinal das despesas (de positivo para negativo)
            for periodo in periodos:
                expenses[periodo] = -expenses[periodo]
            expenses["Total"] = -expenses["Total"]
            
            linhas_dre.append(self._criar_linha_titulo("(-) CUSTOS E DESPESAS", periodos))
            
            # Custos
            custos = expenses[expenses["BC_GROUP"].str.contains("Custos", na=False)]
            if not custos.empty:
                linhas_dre.append(self._criar_linha_subtotal("  (-) Custos", custos, periodos))
                for _, row in custos.iterrows():
                    nome_cta = str(row.get("NOME_CTA", "Conta desconhecida"))
                    conta = str(row.get("conta", ""))
                    linhas_dre.append(self._criar_linha_conta(f"    {nome_cta} ({conta})", row, periodos))
                linhas_dre.append(self._criar_linha_subtotal("  Total Custos", custos, periodos))
                linhas_dre.append(self._criar_linha_vazia(periodos))
            
            # Despesas Operacionais
            desp_op = expenses[expenses["BC_GROUP"].str.contains("Despesas-Operacionais", na=False)]
            if not desp_op.empty:
                linhas_dre.append(self._criar_linha_subtotal("  (-) Despesas Operacionais", desp_op, periodos))
                for _, row in desp_op.iterrows():
                    nome_cta = str(row.get("NOME_CTA", "Conta desconhecida"))
                    conta = str(row.get("conta", ""))
                    linhas_dre.append(self._criar_linha_conta(f"    {nome_cta} ({conta})", row, periodos))
                linhas_dre.append(self._criar_linha_subtotal("  Total Despesas Operacionais", desp_op, periodos))
                linhas_dre.append(self._criar_linha_vazia(periodos))
            
            # Despesas Financeiras
            desp_fin = expenses[expenses["BC_GROUP"].str.contains("Despesas-Financeiras", na=False)]
            if not desp_fin.empty:
                linhas_dre.append(self._criar_linha_subtotal("  (-) Despesas Financeiras", desp_fin, periodos))
                for _, row in desp_fin.iterrows():
                    nome_cta = str(row.get("NOME_CTA", "Conta desconhecida"))
                    conta = str(row.get("conta", ""))
                    linhas_dre.append(self._criar_linha_conta(f"    {nome_cta} ({conta})", row, periodos))
                linhas_dre.append(self._criar_linha_subtotal("  Total Despesas Financeiras", desp_fin, periodos))
                linhas_dre.append(self._criar_linha_vazia(periodos))
            
            # Outras Despesas
            outras_desp = expenses[
                ~expenses["BC_GROUP"].str.contains("Custos", na=False) &
                ~expenses["BC_GROUP"].str.contains("Despesas-Operacionais", na=False) &
                ~expenses["BC_GROUP"].str.contains("Despesas-Financeiras", na=False)
            ]
            if not outras_desp.empty:
                linhas_dre.append(self._criar_linha_subtotal("  (-) Outras Despesas", outras_desp, periodos))
                for _, row in outras_desp.iterrows():
                    nome_cta = str(row.get("NOME_CTA", "Conta desconhecida"))
                    conta = str(row.get("conta", ""))
                    linhas_dre.append(self._criar_linha_conta(f"    {nome_cta} ({conta})", row, periodos))
                linhas_dre.append(self._criar_linha_subtotal("  Total Outras Despesas", outras_desp, periodos))
                linhas_dre.append(self._criar_linha_vazia(periodos))
            
            total_despesas = expenses["Total"].sum()
            linhas_dre.append(self._criar_linha_total("TOTAL DESPESAS", expenses, periodos))
            linhas_dre.append(self._criar_linha_vazia(periodos))
        
        # Resultado
        total_receitas_val = income["Total"].sum() if not income.empty else 0
        total_despesas_val = expenses["Total"].sum() if not expenses.empty else 0
        resultado_total = total_receitas_val + total_despesas_val  # Despesas já são negativas
        
        # Calcula resultado por período
        linha_resultado = {"Item": "RESULTADO DO PERÍODO"}
        for periodo in periodos:
            receita_periodo = income[periodo].sum() if not income.empty else 0.0
            despesa_periodo = expenses[periodo].sum() if not expenses.empty else 0.0  # Já é negativo
            linha_resultado[periodo] = receita_periodo + despesa_periodo  # Despesas já são negativas
        linha_resultado["Total"] = resultado_total
        linhas_dre.append(linha_resultado)
        
        # Cria DataFrame e garante ordem das colunas: Item + períodos + Total
        df_result = pd.DataFrame(linhas_dre)
        
        # Garante que "Total" é a última coluna
        colunas_ordenadas = ["Item"] + periodos + ["Total"]
        # Filtra apenas colunas que existem no DataFrame
        colunas_ordenadas = [c for c in colunas_ordenadas if c in df_result.columns]
        
        return df_result[colunas_ordenadas]
    
    def _criar_linha_titulo(self, titulo: str, periodos: List[str]) -> Dict:
        """Cria linha de título sem valores."""
        linha = {"Item": titulo}
        for periodo in periodos:
            linha[periodo] = None
        linha["Total"] = None
        return linha
    
    def _criar_linha_vazia(self, periodos: List[str]) -> Dict:
        """Cria linha vazia."""
        linha = {"Item": ""}
        for periodo in periodos:
            linha[periodo] = None
        linha["Total"] = None
        return linha
    
    def _criar_linha_subtotal(self, item: str, df: pd.DataFrame, periodos: List[str], negativar: bool = False) -> Dict:
        """Cria linha de subtotal."""
        linha = {"Item": item}
        for periodo in periodos:
            valor = df[periodo].sum()
            linha[periodo] = valor
        total = df["Total"].sum()
        linha["Total"] = total
        return linha
    
    def _criar_linha_total(self, item: str, df: pd.DataFrame, periodos: List[str], negativar: bool = False) -> Dict:
        """Cria linha de total."""
        return self._criar_linha_subtotal(item, df, periodos, negativar)
    
    def _criar_linha_conta(self, item: str, row: pd.Series, periodos: List[str]) -> Dict:
        """Cria linha de conta individual."""
        linha = {"Item": item}
        for periodo in periodos:
            linha[periodo] = row.get(periodo, 0.0)
        linha["Total"] = row.get("Total", 0.0)
        return linha
    
    def _debug_unknown_accounts(self, df_dre: pd.DataFrame) -> None:
        """Alerta sobre contas classificadas como Unknown."""
        if "BC_GROUP" not in df_dre.columns:
            return  # Se BC_GROUP não existe, não há nada para debugar
        
        contas_unknown = df_dre[df_dre["BC_GROUP"] == "Unknown"]
        if not contas_unknown.empty:
            print(
                f"\n[DEBUG] {len(contas_unknown)} conta(s) classificada(s) como 'Unknown' na DRE:",
                file=sys.stderr
            )
            for _, row in contas_unknown.iterrows():
                conta = row.get("conta", "?")
                nome = row.get("NOME_CTA", "?")
                clas_cta = row.get("CLAS_CTA", "?")
                movimento = row.get("movimento", 0)
                print(
                    f"  [DEBUG] Conta {conta} | {nome} | CLAS_CTA={clas_cta} | Movimento={movimento:.2f}",
                    file=sys.stderr
                )


class TrialBalanceBuilder:
    """
    Construtor de Balancete.
    
    Gera balancete com saldo inicial, débitos, créditos e saldo final.
    """
    
    def __init__(
        self,
        df_plano_contas: pd.DataFrame,
        df_saldos_iniciais: pd.DataFrame,
        df_lancamentos: pd.DataFrame,
        account_mapper: AccountMapper
    ):
        """
        Inicializa o construtor de Balancete.
        
        Args:
            df_plano_contas: DataFrame com plano de contas (deve ter colunas: CODI_CTA, NOME_CTA, CLAS_CTA)
            df_saldos_iniciais: DataFrame com saldos iniciais (deve ter colunas: conta, saldo)
            df_lancamentos: DataFrame com lançamentos do período (deve ter colunas: cdeb_lan, ccre_lan, vlor_lan)
            account_mapper: Instância de AccountMapper (não usado diretamente, mas mantido para compatibilidade)
        """
        self.df_plano_contas = df_plano_contas
        self.df_saldos_iniciais = df_saldos_iniciais
        self.df_lancamentos = df_lancamentos
        self.account_mapper = account_mapper
    
    def gerar(self) -> pd.DataFrame:
        """
        Gera balancete com saldo inicial, débitos, créditos e saldo final.
        
        Returns:
            DataFrame com colunas: Código, Nome, Classificação, 
                                   Saldo Inicial, Total Débitos, Total Créditos, Saldo Final
        """
        # Inicia com plano de contas
        df_balancete = self.df_plano_contas[["CODI_CTA", "NOME_CTA", "CLAS_CTA"]].copy()
        df_balancete["CODI_CTA"] = df_balancete["CODI_CTA"].astype(str)
        
        # Mescla saldos iniciais
        if self.df_saldos_iniciais is not None and not self.df_saldos_iniciais.empty:
            df_saldos_init = self.df_saldos_iniciais.copy()
            df_saldos_init["conta"] = df_saldos_init["conta"].astype(str)
            df_balancete = df_balancete.merge(
                df_saldos_init[["conta", "saldo"]],
                left_on="CODI_CTA",
                right_on="conta",
                how="left"
            )
            df_balancete["Saldo Inicial"] = pd.to_numeric(df_balancete["saldo"], errors="coerce").fillna(0.0)
            df_balancete = df_balancete.drop(columns=["conta", "saldo"], errors="ignore")
        else:
            df_balancete["Saldo Inicial"] = 0.0
        
        # Calcula débitos do período
        if self.df_lancamentos is not None and not self.df_lancamentos.empty:
            # Filtra linhas com débito (cdeb_lan != 0)
            df_debitos = self.df_lancamentos[
                (self.df_lancamentos["cdeb_lan"].astype(str).str.strip() != "0") &
                (self.df_lancamentos["cdeb_lan"].notna())
            ].copy()
            
            if not df_debitos.empty:
                df_debitos["cdeb_lan"] = df_debitos["cdeb_lan"].astype(str).str.strip()
                debitos_agrupados = df_debitos.groupby("cdeb_lan")["vlor_lan"].sum().reset_index()
                debitos_agrupados.columns = ["conta", "Total Débitos"]
                debitos_agrupados["conta"] = debitos_agrupados["conta"].astype(str).str.strip()
                df_balancete = df_balancete.merge(
                    debitos_agrupados,
                    left_on="CODI_CTA",
                    right_on="conta",
                    how="left"
                )
                df_balancete["Total Débitos"] = pd.to_numeric(df_balancete["Total Débitos"], errors="coerce").fillna(0.0)
                df_balancete = df_balancete.drop(columns=["conta"], errors="ignore")
            else:
                df_balancete["Total Débitos"] = 0.0
            
            # Calcula créditos do período
            # Filtra linhas com crédito (ccre_lan != 0)
            df_creditos = self.df_lancamentos[
                (self.df_lancamentos["ccre_lan"].astype(str).str.strip() != "0") &
                (self.df_lancamentos["ccre_lan"].notna())
            ].copy()
            
            if not df_creditos.empty:
                df_creditos["ccre_lan"] = df_creditos["ccre_lan"].astype(str).str.strip()
                creditos_agrupados = df_creditos.groupby("ccre_lan")["vlor_lan"].sum().reset_index()
                creditos_agrupados.columns = ["conta", "Total Créditos"]
                creditos_agrupados["conta"] = creditos_agrupados["conta"].astype(str).str.strip()
                df_balancete = df_balancete.merge(
                    creditos_agrupados,
                    left_on="CODI_CTA",
                    right_on="conta",
                    how="left"
                )
                df_balancete["Total Créditos"] = pd.to_numeric(df_balancete["Total Créditos"], errors="coerce").fillna(0.0)
                df_balancete = df_balancete.drop(columns=["conta"], errors="ignore")
            else:
                df_balancete["Total Créditos"] = 0.0
        else:
            df_balancete["Total Débitos"] = 0.0
            df_balancete["Total Créditos"] = 0.0
        
        # Calcula saldo final = saldo inicial + débitos - créditos
        df_balancete["Saldo Final"] = (
            df_balancete["Saldo Inicial"] + 
            df_balancete["Total Débitos"] - 
            df_balancete["Total Créditos"]
        )
        
        # Arredonda valores para 2 casas decimais
        df_balancete["Saldo Inicial"] = df_balancete["Saldo Inicial"].round(2)
        df_balancete["Total Débitos"] = df_balancete["Total Débitos"].round(2)
        df_balancete["Total Créditos"] = df_balancete["Total Créditos"].round(2)
        df_balancete["Saldo Final"] = df_balancete["Saldo Final"].round(2)
        
        # Renomeia colunas para o formato final
        df_balancete = df_balancete.rename(columns={
            "CODI_CTA": "Código",
            "NOME_CTA": "Nome",
            "CLAS_CTA": "Classificação"
        })
        
        # Ordena por classificação
        df_balancete = df_balancete.sort_values("Classificação")
        
        # Seleciona apenas colunas necessárias na ordem correta
        df_balancete = df_balancete[[
            "Código", "Nome", "Classificação", 
            "Saldo Inicial", "Total Débitos", "Total Créditos", "Saldo Final"
        ]]
        
        return df_balancete


class PeriodMovementsBuilder:
    """
    Construtor de Extrato/Movimentação do Período.
    
    Prepara movimentações do período para exportação.
    """
    
    def __init__(
        self,
        df_lancamentos: pd.DataFrame,
        account_mapper: AccountMapper
    ):
        """
        Inicializa o construtor de Extrato.
        
        Args:
            df_lancamentos: DataFrame com lançamentos do período (deve ter colunas: data_lan, cdeb_lan, ccre_lan, 
                           chis_lan, ndoc_lan, codi_lote, vlor_lan, e opcionalmente Conta Débito, Conta Crédito)
            account_mapper: Instância de AccountMapper para mapear contas (não usado diretamente, mas mantido para compatibilidade)
        """
        self.df_lancamentos = df_lancamentos
        self.account_mapper = account_mapper
    
    def gerar(self) -> pd.DataFrame:
        """
        Gera extrato formatado do período.
        
        Returns:
            DataFrame com colunas: data_lan, Código Débito, Conta Débito, Código Crédito, Conta Crédito, 
                                   chis_lan, ndoc_lan, codi_lote, vlor_lan
        """
        if self.df_lancamentos is None or self.df_lancamentos.empty:
            return pd.DataFrame()
        
        df_mov_export = self.df_lancamentos.copy()
        
        # Garante que as colunas necessárias existam
        if "Conta Débito" not in df_mov_export.columns:
            df_mov_export["Conta Débito"] = ""
        if "Conta Crédito" not in df_mov_export.columns:
            df_mov_export["Conta Crédito"] = ""
        
        # Adiciona colunas de código (cdeb_lan e ccre_lan devem estar no DataFrame)
        if "cdeb_lan" not in df_mov_export.columns:
            df_mov_export["cdeb_lan"] = ""
        if "ccre_lan" not in df_mov_export.columns:
            df_mov_export["ccre_lan"] = ""
        
        # Cria colunas de código formatadas como string
        def limpar_codigo(valor):
            if pd.isna(valor):
                return ""
            str_val = str(valor).strip()
            if str_val in ["0", "nan", "None", ""]:
                return ""
            return str_val
        
        df_mov_export["Código Débito"] = df_mov_export["cdeb_lan"].apply(limpar_codigo)
        df_mov_export["Código Crédito"] = df_mov_export["ccre_lan"].apply(limpar_codigo)
        
        # Seleciona colunas disponíveis na ordem desejada
        colunas_desejadas = ["data_lan", "Código Débito", "Conta Débito", "Código Crédito", "Conta Crédito", "chis_lan", "ndoc_lan", "codi_lote", "vlor_lan"]
        colunas_disponiveis = [col for col in colunas_desejadas if col in df_mov_export.columns]
        
        df_mov_export = df_mov_export[colunas_disponiveis].copy()
        
        # Ordena se possível
        if "data_lan" in df_mov_export.columns:
            if "codi_lote" in df_mov_export.columns:
                df_mov_export = df_mov_export.sort_values(["data_lan", "codi_lote"])
            else:
                df_mov_export = df_mov_export.sort_values("data_lan")
        
        return df_mov_export

