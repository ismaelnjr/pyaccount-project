#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo para classificação de contas contábeis em categorias Beancount.

Permite configuração customizada por empresa, com valores padrão.
"""
from typing import Dict, Optional
import configparser


# Configuração padrão de classificação
CLASSIFICACAO_M1: Dict[str, str] = {
    # 1 - Ativo
    "1":  "Assets:Ativo",                      # Ativo geral
    "11": "Assets:Ativo-Circulante",           # Ativo Circulante
    "12": "Assets:Ativo-Nao-Circulante",       # Ativo Não Circulante
    
    # 2 - Passivo e Patrimônio Líquido
    "2":  "Liabilities:Passivo",               # Passivo geral
    "21": "Liabilities:Passivo-Circulante",    # Passivo Circulante
    "22": "Liabilities:Passivo-Nao-Circulante",# Passivo Não Circulante
    "23": "Equity:Patrimonio-Liquido",         # Patrimônio Líquido
    
    # 3 - Custos e Despesas
    "3":  "Expenses:Custos-Despesas",          # Agrupamento geral
    "31": "Expenses:Custos",                   # Custos (CPV, CMP)
    "32": "Expenses:Despesas-Operacionais",    # Despesas operacionais
    "33": "Expenses:Despesas-Financeiras",     # Despesas financeiras
    "34": "Expenses:Outras-Despesas",          # Outras despesas
    
    # 4 - Receitas
    "4":  "Income:Receitas",                   # Receita geral
    "41": "Income:Receitas-Operacionais",      # Receita operacional
    "42": "Income:Receitas-Financeiras",       # Receita financeira
    "43": "Income:Outras-Receitas",            # Outras receitas
    
    # 5 - Contas Transitórias
    "5":  "Equity:Contas-Transitorias",     # Ex: contas de fechamento / apuração
    
    # 9 - Contas de Compensação
    "9":  "Equity:Contas-Compensacao"      # Contas de controle / não patrimoniais (usando Equity como tipo válido do Beancount)
}


def classificar_conta(
    clas_cta: str, 
    tipo_cta: Optional[str] = None,
    mapeamento_contas: Optional[Dict[str, str]] = None
) -> str:
    """
    Classifica conta contábil em categoria Beancount baseado em CLAS_CTA.
    
    Usa mapeamento customizado se fornecido, caso contrário usa a configuração padrão.
    Os prefixos mais longos são verificados primeiro (ex: "31" antes de "3").
    
    Args:
        clas_cta: Classificação da conta (ex: "11210100708", "311203", "4")
        tipo_cta: Tipo da conta ('A' = analítica, 'S' = sintética) - não usado para classificação
        mapeamento_contas: Dicionário opcional com prefixos e categorias Beancount customizados
                                Ex: {"1": "Assets", "2": "Liabilities", ...}
    
    Returns:
        Nome da categoria Beancount (Assets, Liabilities, Income, Expenses, etc.)
    """
    # Usa mapeamento customizado ou padrão
    mapeamento = mapeamento_contas if mapeamento_contas else CLASSIFICACAO_M1
    
    # Converte CLAS_CTA para string para garantir comparação correta
    clas = str(clas_cta or "").strip()
    
    if not clas:
        return "Unknown"
    
    # Ordena prefixos por comprimento (maior primeiro) para verificar os mais específicos primeiro
    prefixos_ordenados = sorted(mapeamento.keys(), key=len, reverse=True)
    
    # Verifica prefixos específicos primeiro
    for prefixo in prefixos_ordenados:
        if clas.startswith(prefixo):
            return mapeamento[prefixo]
    
    return "Unknown"


def carregar_classificacao_do_config(config: Dict) -> Optional[Dict[str, str]]:
    """
    Carrega configuração de classificação de um dicionário de configuração.
    
    Args:
        config: Dicionário de configuração com chaves no formato "clas_<prefixo>"
                Ex: {"clas_1": "Assets", "clas_2": "Liabilities", ...}
    
    Returns:
        Dicionário de mapeamento ou None se não houver configuração customizada
    """
    mapeamento = {}
    
    for chave, valor in config.items():
        if chave.startswith("clas_") and chave != "clas_cta":
            prefixo = chave.replace("clas_", "")
            mapeamento[prefixo] = valor.strip()
    
    return mapeamento if mapeamento else None


def carregar_classificacao_do_ini(config_path: str, section: str = "classification") -> Optional[Dict[str, str]]:
    """
    Carrega configuração de classificação de um arquivo INI.
    
    Args:
        config_path: Caminho do arquivo INI
        section: Nome da seção no arquivo INI (default: "classification")
    
    Returns:
        Dicionário de mapeamento ou None se não houver configuração customizada
    """
    cfg = configparser.ConfigParser()
    cfg.read(config_path)
    
    if not cfg.has_section(section):
        return None
    
    mapeamento = {}
    for chave, valor in cfg.items(section):
        if chave.startswith("clas_"):
            prefixo = chave.replace("clas_", "")
            mapeamento[prefixo] = valor.strip()
    
    return mapeamento if mapeamento else None

