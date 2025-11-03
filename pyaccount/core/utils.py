#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utilitários compartilhados para o módulo pyaccount.
"""
import pandas as pd
import re


def normalizar_nome(nome: str) -> str:
    """
    Normaliza nome da conta removendo acentos, parênteses, pontos e caracteres especiais.
    Usa hífen no lugar de underscore.
    
    Args:
        nome: Nome da conta original
        
    Returns:
        Nome normalizado para Beancount no formato Capital-Case
    """
    if pd.isna(nome): 
        return "Sem-Nome"
    
    s = str(nome).strip()
    
    # Trata contas "contra-ativo" que começam com "(-)"
    if s.startswith("(-)"):
        s = s.replace("(-)", "Redutora ")
    
    # Remove acentos
    repl = {
        "ç": "c", "ã": "a", "á": "a", "à": "a", "â": "a",
        "é": "e", "ê": "e", "í": "i", "ó": "o", "ô": "o", "õ": "o", "ú": "u",
        "Ç": "C", "Ã": "A", "Á": "A", "À": "A", "Â": "A",
        "É": "E", "Ê": "E", "Í": "I", "Ó": "O", "Ô": "O", "Õ": "O", "Ú": "U"
    }
    for k, v in repl.items():
        s = s.replace(k, v)
    
    # Remove parênteses (substitui por espaço)
    s = s.replace("(", " ").replace(")", " ")
    
    # Substitui underscore e barra por hífen
    s = s.replace("_", "-").replace("/", "-")
    
    # Remove ponto entre números (ex: "10.833" -> "10833")
    s = re.sub(r"(\d)\.(\d)", r"\1\2", s)
    
    # Substitui outros pontos por hífen
    s = s.replace(".", "-")
    
    # Remove caracteres especiais (mantém apenas letras, números, hífen e espaço)
    s = re.sub(r"[^A-Za-z0-9\- ]+", " ", s)
    
    # Divide em tokens e capitaliza cada um
    tokens = [t for t in re.split(r"\s+", s) if t]
    s = "-".join([t.capitalize() for t in tokens])
    
    return s or "Sem-Nome"


def fmt_amount(v: float, cur: str) -> str:
    """
    Formata valor numérico para formato Beancount simples.
    
    Args:
        v: Valor numérico
        cur: Código da moeda (ex: "BRL")
        
    Returns:
        String formatada no formato "665650.84 BRL" (ponto decimal, sem separador de milhar)
    """
    return f"{v:.2f} {cur}"


def format_val(v: float, currency: str) -> str:
    """
    Alias para fmt_amount para compatibilidade com código existente.
    
    Args:
        v: Valor numérico
        currency: Código da moeda (ex: "BRL")
        
    Returns:
        String formatada no formato "665650.84 BRL"
    """
    return fmt_amount(v, currency)
