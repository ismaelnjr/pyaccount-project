#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo para mapeamento de contas contábeis para Beancount.

Classe base compartilhada para processamento de planos de contas e mapeamento.
"""
from typing import Dict, Optional
import pandas as pd

from pyaccount.core.account_classifier import AccountClassifier
from pyaccount.core.utils import normalizar_nome


class AccountMapper:
    """
    Classe base para mapeamento de contas contábeis para Beancount.
    
    Encapsula a lógica comum de:
    - Classificação de contas (CLAS_CTA -> BC_GROUP)
    - Normalização de nomes de contas
    - Criação de contas Beancount hierárquicas (BC_ACCOUNT)
    - Criação de mapas de lookup (codi_to_bc, clas_to_bc)
    """
    
    def __init__(
        self, 
        classificacao_customizada: Optional[Dict[str, str]] = None
    ):
        """
        Inicializa o mapeador de contas.
        
        Args:
            classificacao_customizada: Dicionário opcional com mapeamento customizado
                                      de prefixos CLAS_CTA para categorias Beancount.
                                      Se None, usa CLASSIFICACAO_PADRAO_BR.
        """
        self.classifier = AccountClassifier(classificacao_customizada)
        self.custom_classifier = classificacao_customizada
    
    def classificar_beancount(self, clas_cta: str, tipo_cta: Optional[str] = None) -> str:
        """
        Mapeia CLAS_CTA -> grupo Beancount.
        
        Args:
            clas_cta: Classificação da conta
            tipo_cta: Tipo da conta ('A' = analítica, 'S' = sintética) - não usado para classificação
        
        Returns:
            Nome da categoria Beancount
        """
        return self.classifier.classificar(clas_cta, tipo_cta)
    
    def criar_bc_account(self, bc_group: str, bc_name: str) -> str:
        """
        Cria nome completo de conta Beancount a partir de grupo e nome.
        
        Se BC_GROUP já contém ":", apenas concatena com BC_NAME usando ":".
        Caso contrário, normaliza BC_GROUP e adiciona ":".
        
        Args:
            bc_group: Grupo/categoria Beancount (ex: "Assets:Ativo-Circulante")
            bc_name: Nome normalizado da conta (ex: "Caixa")
        
        Returns:
            Nome completo da conta Beancount (ex: "Assets:Ativo-Circulante:Caixa")
        """
        bc_group_str = str(bc_group)
        bc_name_str = str(bc_name)
        
        if ":" in bc_group_str:
            # BC_GROUP já está no formato hierárquico, apenas adiciona BC_NAME
            return bc_group_str + ":" + bc_name_str
        else:
            # BC_GROUP precisa ser normalizado e então adiciona BC_NAME
            bc_group_norm = normalizar_nome(bc_group_str)
            return bc_group_norm + ":" + bc_name_str
    
    def processar_plano_contas(
        self,
        df_pc: pd.DataFrame,
        filtrar_ativas: bool = False
    ) -> pd.DataFrame:
        """
        Processa plano de contas e aplica mapeamento para Beancount.
        
        Adiciona colunas:
        - BC_GROUP: Categoria Beancount baseada em CLAS_CTA
        - BC_NAME: Nome normalizado da conta
        - BC_ACCOUNT: Nome completo hierárquico da conta Beancount
        
        Args:
            df_pc: DataFrame com plano de contas (deve conter CLAS_CTA, TIPO_CTA, NOME_CTA)
            filtrar_ativas: Se True, filtra apenas contas com SITUACAO_CTA = 'A'
        
        Returns:
            DataFrame processado com colunas BC_GROUP, BC_NAME, BC_ACCOUNT
        """
        if df_pc.empty:
            raise ValueError("DataFrame do plano de contas está vazio.")
        
        # Filtra apenas contas ativas se solicitado
        if filtrar_ativas and "SITUACAO_CTA" in df_pc.columns:
            df_pc = df_pc[df_pc["SITUACAO_CTA"].astype(str).str.upper().eq("A")].copy()
        
        # Aplica classificação Beancount
        df_pc["BC_GROUP"] = [
            self.classificar_beancount(clas, tipo) 
            for clas, tipo in zip(df_pc["CLAS_CTA"], df_pc["TIPO_CTA"])
        ]
        
        # Normaliza nomes
        df_pc["BC_NAME"] = df_pc["NOME_CTA"].astype(str).apply(normalizar_nome)
        
        # Cria BC_ACCOUNT usando método helper
        df_pc["BC_ACCOUNT"] = df_pc.apply(
            lambda row: self.criar_bc_account(row["BC_GROUP"], row["BC_NAME"]),
            axis=1
        )
        
        return df_pc
    
    def criar_mapas(self, df_pc: pd.DataFrame) -> Dict[str, Dict[str, str]]:
        """
        Cria mapas de lookup para contas Beancount.
        
        Args:
            df_pc: DataFrame com plano de contas processado (deve conter CLAS_CTA, CODI_CTA, BC_ACCOUNT)
        
        Returns:
            Dicionário com mapas:
            - "clas_to_bc": Mapeamento CLAS_CTA -> BC_ACCOUNT
            - "codi_to_bc": Mapeamento CODI_CTA -> BC_ACCOUNT
        """
        mapas = {}
        
        # Mapa por classificação (CLAS_CTA) -> BC_ACCOUNT
        mapas["clas_to_bc"] = dict(zip(df_pc["CLAS_CTA"].astype(str), df_pc["BC_ACCOUNT"]))
        
        # Mapa por código de conta (CODI_CTA) -> BC_ACCOUNT
        mapas["codi_to_bc"] = dict(zip(df_pc["CODI_CTA"].astype(str), df_pc["BC_ACCOUNT"]))
        
        return mapas

