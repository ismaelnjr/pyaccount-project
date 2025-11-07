#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo de logging para consultas SQL.

Fornece funcionalidades para registrar queries SQL executadas pelos clientes de dados.
"""
import re
from datetime import datetime, date
from typing import Optional, List, Any
from pathlib import Path


def _substituir_parametros_sql(sql: str, params: Optional[List[Any]]) -> str:
    """
    Substitui placeholders (?) na query SQL pelos valores dos parâmetros.
    
    Args:
        sql: Query SQL com placeholders (?)
        params: Lista de parâmetros para substituir
        
    Returns:
        SQL com parâmetros substituídos e formatados
    """
    if not params:
        return sql
    
    sql_formatado = sql
    for param in params:
        if param is None:
            valor = "NULL"
        elif isinstance(param, str):
            # Escapa aspas simples e adiciona aspas
            valor_escape = param.replace("'", "''")
            valor = f"'{valor_escape}'"
        elif isinstance(param, (int, float)):
            valor = str(param)
        elif isinstance(param, datetime):
            valor = f"'{param.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif isinstance(param, date):
            valor = f"'{param.strftime('%Y-%m-%d')}'"
        else:
            valor = f"'{str(param)}'"
        
        # Substitui o primeiro ? encontrado
        sql_formatado = sql_formatado.replace("?", valor, 1)
    
    return sql_formatado


def log_query(sql: str, params: Optional[List[Any]] = None, log_file: str = "logs/queries.log") -> None:
    """
    Registra uma query SQL em arquivo de log.
    
    Args:
        sql: Query SQL a ser logada
        params: Parâmetros da query (opcional)
        log_file: Caminho do arquivo de log (padrão: logs/queries.log)
    """
    # Cria diretório se não existir
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Substitui parâmetros na query
    sql_formatado = _substituir_parametros_sql(sql, params)
    
    # Normaliza query para uma única linha: remove quebras de linha e espaços múltiplos
    sql_formatado = re.sub(r'\s+', ' ', sql_formatado)  # Substitui espaços múltiplos por um único espaço
    sql_formatado = sql_formatado.strip()  # Remove espaços no início e fim
    
    # Gera timestamp com milissegundos
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    # Formata linha do log: [TIMESTAMP] SQL_QUERY
    linha_log = f"[{timestamp}] {sql_formatado}\n"
    
    # Escreve no arquivo (modo append)
    try:
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(linha_log)
    except Exception as e:
        # Não interrompe a execução se houver erro ao escrever log
        print(f"[WARNING] Erro ao escrever log de query: {e}", file=__import__('sys').stderr)

