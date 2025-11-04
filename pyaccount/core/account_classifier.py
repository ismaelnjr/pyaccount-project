#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo para classificação de contas contábeis em categorias Beancount.

Permite configuração customizada por empresa, com valores padrão.
Suporta múltiplos modelos de classificação baseados no tipo de plano de contas.
"""
from enum import Enum
from typing import Dict, Optional, List
import configparser


class TipoPlanoContas(str, Enum):
    """
    Tipos de plano de contas suportados.
    
    Cada tipo representa uma estrutura diferente de classificação contábil.
    """
    PADRAO = "padrao"              # Padrão brasileiro (estrutura atual)
    SIMPLIFICADO = "simplificado"  # Estrutura simplificada
    IFRS = "ifrs"                  # Classificação IFRS


# ============================================================================
# MODELOS DE CLASSIFICAÇÃO
# ============================================================================

# Modelo padrão brasileiro de classificação contábil.
# Baseado na estrutura tradicional de planos de contas brasileiros.
CLASSIFICACAO_PADRAO_BR: Dict[str, str] = {
    # 1 - Ativo
    "1":  "Assets:Ativo",                      # Ativo geral
    "11": "Assets:Ativo-Circulante",           # Ativo Circulante
    "12": "Assets:Ativo-Nao-Circulante",       # Ativo Não Circulante
    
    # 2 - Passivo e Patrimônio Líquido
    "2":  "Liabilities:Passivo",               # Passivo geral
    "21": "Liabilities:Passivo-Circulante",    # Passivo Circulante
    "22": "Liabilities:Passivo-Nao-Circulante", # Passivo Não Circulante
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

# Modelo simplificado com estrutura básica.
# Útil para empresas pequenas ou planos de contas simplificados.
CLASSIFICACAO_SIMPLIFICADO: Dict[str, str] = {
    "1": "Assets:Ativo",
    "11": "Assets:Ativo-Circulante",
    "12": "Assets:Ativo-Nao-Circulante",
    "2": "Liabilities",
    "21": "Liabilities:Passivo-Circulante",
    "22": "Liabilities:Passivo-Nao-Circulante",
    "23": "Equity:Patrimonio-Liquido",
    "9": "Income:Receitas",
    "91": "Income:Receitas-Operacionais",
    "92": "Income:Abatimentos-Receitas",
    "93": "Expenses:Custos-dos-Bens-e-Servicos-Vendidos",
    "94": "Expenses:Despesas-Operacionais",
    "95": "Expenses:Resultado-Nao-Operacional",
    "96": "Expenses:Provisao-Imposto-de-Renda",
    "97": "Expenses:Provisao-Contribuicao-Social",
    "98": "Expenses:Provisao-Outras",
    "99": "Expenses:Apuracao-Resultado",
}

# Modelo baseado em IFRS (International Financial Reporting Standards).
# Adapte conforme necessário para seu contexto.
CLASSIFICACAO_IFRS: Dict[str, str] = {
    # Ativo
    "1":    "Assets",
    "11":   "Assets:Current",
    "111":  "Assets:Current:CashAndCashEquivalents",
    "112":  "Assets:Current:AccountsReceivable",
    "113":  "Assets:Current:Inventories",
    "114":  "Assets:Current:OtherCurrentAssets",
    "12":   "Assets:Non-Current",
    "121":  "Assets:Non-Current:PropertyPlantAndEquipment",
    "122":  "Assets:Non-Current:IntangibleAssets",
    "123":  "Assets:Non-Current:Investments",
    "124":  "Assets:Non-Current:DeferredTaxAssets",

    # Passivo
    "2":    "Liabilities",
    "21":   "Liabilities:Current",
    "211":  "Liabilities:Current:Suppliers",
    "212":  "Liabilities:Current:LoansAndFinancing",
    "213":  "Liabilities:Current:TaxesPayable",
    "214":  "Liabilities:Current:Provisions",
    "22":   "Liabilities:Non-Current",
    "221":  "Liabilities:Non-Current:LoansAndFinancing",
    "222":  "Liabilities:Non-Current:Provisions",
    "223":  "Liabilities:Non-Current:DeferredTaxLiabilities",

    # Patrimônio líquido
    "3":    "Equity",
    "31":   "Equity:CapitalStock",
    "32":   "Equity:Reserves",
    "33":   "Equity:RetainedEarnings",

    # Resultado
    "4":    "Income",
    "41":   "Income:SalesRevenue",
    "42":   "Income:OtherOperatingIncome",
    "43":   "Income:FinancialIncome",

    "5":    "Expenses",
    "51":   "Expenses:CostOfGoodsSold",
    "52":   "Expenses:OperatingExpenses",
    "53":   "Expenses:AdministrativeExpenses",
    "54":   "Expenses:FinancialExpenses",
    "55":   "Expenses:TaxesAndContributions",
}

# ============================================================================
# REGISTRY DE MODELOS
# ============================================================================

MODELOS_CLASSIFICACAO: Dict[TipoPlanoContas, Dict[str, str]] = {
    TipoPlanoContas.PADRAO: CLASSIFICACAO_PADRAO_BR,
    TipoPlanoContas.SIMPLIFICADO: CLASSIFICACAO_SIMPLIFICADO,
    TipoPlanoContas.IFRS: CLASSIFICACAO_IFRS,
}


def obter_classificacao_do_modelo(
    modelo: Optional[TipoPlanoContas] = None,
    customizacoes: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """
    Obtém o dicionário de classificação baseado no modelo, aplicando customizações se fornecidas.
    
    Args:
        modelo: Tipo de plano de contas. Se None, usa CLASSIFICACAO_PADRAO_BR.
        customizacoes: Dicionário opcional com customizações. Se fornecido, sobrescreve valores do modelo.
    
    Returns:
        Dicionário de classificação final (modelo + customizações)
    """
    # Determina o dicionário base
    if modelo and modelo in MODELOS_CLASSIFICACAO:
        classificacao = MODELOS_CLASSIFICACAO[modelo].copy()
    else:
        classificacao = CLASSIFICACAO_PADRAO_BR.copy()
    
    # Aplica customizações se houver (têm prioridade)
    if customizacoes:
        classificacao.update(customizacoes)
    
    return classificacao


class AccountClassifier:
    """
    Classificador de contas contábeis em categorias Beancount.
    
    Permite configuração customizada por empresa, com valores padrão.
    Suporta múltiplos modelos de classificação baseados no tipo de plano de contas.
    """
    
    def __init__(
        self, 
        mapeamento_customizado: Optional[Dict[str, str]] = None
    ):
        """
        Inicializa o classificador.
        
        Args:
            mapeamento_customizado: Dicionário com prefixos e categorias Beancount.
                                   Se None, usa CLASSIFICACAO_PADRAO_BR.
        """
        if mapeamento_customizado:
            self.mapeamento = mapeamento_customizado
        else:
            self.mapeamento = CLASSIFICACAO_PADRAO_BR
        
        # Ordena prefixos por comprimento (maior primeiro) para verificar os mais específicos primeiro
        self.prefixos = sorted(self.mapeamento.keys(), key=len, reverse=True)
    
    def classificar(self, clas_cta: str, tipo_cta: Optional[str] = None) -> str:
        """
        Classifica conta contábil em categoria Beancount baseado em CLAS_CTA.
        
        Usa mapeamento customizado se fornecido, caso contrário usa a configuração padrão.
        Os prefixos mais longos são verificados primeiro (ex: "31" antes de "3").
        
        Args:
            clas_cta: Classificação da conta (ex: "11210100708", "311203", "4")
            tipo_cta: Tipo da conta ('A' = analítica, 'S' = sintética) - não usado para classificação
        
        Returns:
            Nome da categoria Beancount (Assets, Liabilities, Income, Expenses, etc.)
        """
        # Converte CLAS_CTA para string para garantir comparação correta
        clas = str(clas_cta or "").strip()
        
        if not clas:
            return "Unknown"
        
        # Verifica prefixos específicos primeiro
        for prefixo in self.prefixos:
            if clas.startswith(prefixo):
                return self.mapeamento[prefixo]
        
        return "Unknown"
    
    @classmethod
    def carregar_do_config(cls, config: Dict) -> Optional['AccountClassifier']:
        """
        Carrega configuração de classificação de um dicionário de configuração.
        
        Args:
            config: Dicionário de configuração com chaves no formato "clas_<prefixo>"
                    Ex: {"clas_1": "Assets", "clas_2": "Liabilities", ...}
        
        Returns:
            Instância de AccountClassifier ou None se não houver configuração customizada
        """
        mapeamento = {}
        
        for chave, valor in config.items():
            if chave.startswith("clas_") and chave != "clas_cta":
                prefixo = chave.replace("clas_", "")
                mapeamento[prefixo] = valor.strip()
        
        return cls(mapeamento) if mapeamento else None
    
    @classmethod
    def carregar_do_ini(cls, config_path: str, section: str = "classification") -> Optional['AccountClassifier']:
        """
        Carrega configuração de classificação de um arquivo INI.
        
        Args:
            config_path: Caminho do arquivo INI
            section: Nome da seção no arquivo INI (default: "classification")
        
        Returns:
            Instância de AccountClassifier ou None se não houver configuração customizada
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
        
        return cls(mapeamento) if mapeamento else None
    
    @classmethod
    def obter_modelos_disponiveis(cls) -> List[TipoPlanoContas]:
        """
        Retorna lista de modelos disponíveis.
        
        Returns:
            Lista de tipos de plano de contas disponíveis
        """
        return list(MODELOS_CLASSIFICACAO.keys())


