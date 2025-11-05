"""
Utilitários de configuração para testes.
"""
import os
import configparser
from pathlib import Path
from typing import Dict, Optional


def carregar_config_teste(config_path: Optional[str] = None, **overrides) -> Dict[str, str]:
    """
    Carrega configurações do config.ini para uso em testes.
    
    Args:
        config_path: Caminho para config.ini (default: raiz do projeto)
        **overrides: Valores para sobrescrever (ex: empresa=267)
    
    Returns:
        Dict com: dsn, user, password, empresa, moeda, data_inicio, data_fim, saida, modelo, agrupamento_periodo
    """
    # Determina o caminho do config.ini
    if config_path is None:
        # Assume que config.ini está na raiz do projeto
        # test/ está em test/test_config.py, então projeto está em test/..
        test_dir = Path(__file__).parent
        project_root = test_dir.parent
        config_path = project_root / "config.ini"
    else:
        config_path = Path(config_path)
    
    # Carrega configurações do arquivo
    cfg = configparser.ConfigParser()
    cfg.read(config_path)
    
    # Extrai valores das seções
    dsn = cfg.get("database", "dsn", fallback=None)
    user = cfg.get("database", "user", fallback=None)
    password = cfg.get("database", "password", fallback=None)
    moeda = cfg.get("defaults", "moeda", fallback="BRL")
    empresa = cfg.getint("defaults", "empresa", fallback=None)
    data_inicio = cfg.get("defaults", "data_inicio", fallback=None)
    data_fim = cfg.get("defaults", "data_fim", fallback=None)
    saida = cfg.get("defaults", "saida", fallback="./out")
    modelo = cfg.get("defaults", "modelo", fallback="simplificado")
    agrupamento_periodo = cfg.get("defaults", "agrupamento_periodo", fallback=None)
    
    # Cria dicionário com configurações
    config = {
        "dsn": dsn,
        "user": user,
        "password": password,
        "moeda": moeda,
        "empresa": empresa,
        "data_inicio": data_inicio,
        "data_fim": data_fim,
        "saida": saida,
        "modelo": modelo,
        "agrupamento_periodo": agrupamento_periodo if agrupamento_periodo and agrupamento_periodo.lower() != "none" else None
    }
    
    # Aplica overrides se fornecidos
    config.update(overrides)
    
    return config

