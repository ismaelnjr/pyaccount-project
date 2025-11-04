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
    
    # Trata contas "contra-ativo" que começam com "(-)" ou variações com espaços
    # Remove padrões como "(-)", "( - )", "( -)", "(- )" etc.
    padroes_contra_ativo = [
        r"^\(\s*-\s*\)",  # "( - )" no início
        r"^\(\s*-\)",     # "( -)" no início
        r"^\(-\s*\)",     # "(- )" no início
        r"^\(-\)",        # "(-)" no início
    ]
    for padrao in padroes_contra_ativo:
        if re.match(padrao, s):
            # Remove o padrão completamente (sem substituir por nada)
            s = re.sub(padrao, "", s)
            break
    
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
    
    # Remove hífens duplicados/consecutivos (ex: "--" -> "-")
    s = re.sub(r"-+", "-", s)
    
    # Normaliza espaços múltiplos para espaços simples
    s = re.sub(r"\s+", " ", s)
    
    # Remove hífens que estão isolados (rodeados por espaços ou no início/fim)
    # Primeiro, remove hífens no início e fim
    s = s.strip("- ")
    # Depois, remove hífens que são tokens isolados (rodeados por espaços)
    s = re.sub(r"\s-\s", " ", s)  # Remove " - " 
    s = re.sub(r"^\s*-\s+", "", s)  # Remove "- " no início
    s = re.sub(r"\s+-\s*$", "", s)  # Remove " -" no fim
    
    # Divide em tokens por espaços e hífens
    # Primeiro divide por espaços, depois por hífens dentro de cada token
    tokens = []
    for token in s.split():
        token = token.strip()
        if not token or token == "-":
            continue
        # Se o token contém hífens, divide por hífens também
        if "-" in token:
            subtokens = [t.strip() for t in token.split("-") if t.strip()]
            tokens.extend(subtokens)
        else:
            tokens.append(token)
    
    # Capitaliza cada token e junta com hífen
    s = "-".join([t.capitalize() for t in tokens])
    
    # Remove hífens no início e fim do resultado final (caso algum token tenha sido apenas hífen)
    s = s.strip("-")
    
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


