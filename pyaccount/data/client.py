#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface base para acesso a dados contábeis.

Esta classe abstrata define a interface comum para todas as fontes de dados
(banco de dados, arquivos CSV, APIs, etc.), permitindo que o sistema funcione
com diferentes fontes de dados de forma transparente.
"""
from abc import ABC, abstractmethod
from datetime import date
from typing import Optional
import pandas as pd


class DataClient(ABC):
    """
    Interface base abstrata para acesso a dados contábeis.
    
    Define os métodos que todas as implementações de acesso a dados devem fornecer,
    permitindo que o sistema funcione com diferentes fontes (banco de dados, arquivos, etc.).
    """
    
    @abstractmethod
    def buscar_plano_contas(self, empresa: int) -> pd.DataFrame:
        """
        Busca plano de contas para uma empresa específica.
        
        Args:
            empresa: Código da empresa
            
        Returns:
            DataFrame com colunas: CODI_EMP, CODI_CTA, NOME_CTA, CLAS_CTA, 
                                  TIPO_CTA, SITUACAO_CTA
        """
        pass
    
    @abstractmethod
    def buscar_saldos(self, empresa: int, ate: date) -> pd.DataFrame:
        """
        Busca saldos até uma data de corte específica.
        
        Args:
            empresa: Código da empresa
            ate: Data de corte (até quando calcular os saldos)
            
        Returns:
            DataFrame com colunas: conta, saldo
        """
        pass
    
    @abstractmethod
    def buscar_movimentacoes_periodo(self, empresa: int, de: date, ate: date) -> pd.DataFrame:
        """
        Busca movimentações (débitos e créditos) de um período específico.
        
        Args:
            empresa: Código da empresa
            de: Data inicial do período (exclusiva - movimentações após esta data)
            ate: Data final do período (inclusive - movimentações até esta data)
            
        Returns:
            DataFrame com colunas: conta, movimento
            movimento = débitos - créditos (valor positivo aumenta saldo, negativo diminui)
        """
        pass
    
    @abstractmethod
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
                                  ndoc_lan, codi_lote, tipo, codi_usu,
                                  orig_lan, origem_descricao (ou equivalentes)
        """
        pass
    
    # Métodos opcionais para gerenciamento de conexão (implementação padrão vazia)
    def connect(self) -> None:
        """
        Estabelece conexão com a fonte de dados (se necessário).
        
        Implementação padrão vazia para fontes que não precisam de conexão explícita.
        """
        pass
    
    def close(self) -> None:
        """
        Fecha a conexão com a fonte de dados (se necessário).
        
        Implementação padrão vazia para fontes que não precisam de conexão explícita.
        """
        pass
    
    def is_connected(self) -> bool:
        """
        Verifica se está conectado à fonte de dados (se aplicável).
        
        Returns:
            True se conectado ou se não precisa de conexão, False caso contrário
        """
        return True
    
    def __enter__(self):
        """Suporte para context manager (with statement)."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Suporte para context manager (with statement)."""
        self.close()

