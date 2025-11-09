"""
Utilitários de configuração para testes.
"""
import os
import configparser
from pathlib import Path
from typing import Dict, Optional, Any


def carregar_config_teste(config_path: Optional[str] = None, **overrides) -> Dict[str, str]:
    """
    Carrega configurações do config.ini para uso em testes.
    
    Args:
        config_path: Caminho para config.ini (default: raiz do projeto)
        **overrides: Valores para sobrescrever (ex: empresa=267)
    
    Returns:
        Dict com: dsn, user, password, empresa, moeda, data_inicio, data_fim, saida, modelo, agrupamento_periodo,
                  enable_query_log, query_log_file, base_dir, saldos_file, lancamentos_file, plano_contas_file,
                  classificacao_customizada, clas_base
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
    
    # Configurações de logging
    enable_query_log = cfg.getboolean("logging", "enable_query_log", fallback=False)
    query_log_file = cfg.get("logging", "query_log_file", fallback="logs/queries.log")
    
    # Configurações de arquivos (para testes com FileDataClient)
    base_dir = cfg.get("files", "base_dir", fallback="sample_data")
    saldos_file = cfg.get("files", "saldos_file", fallback="saldos_iniciais.CSV")
    lancamentos_file = cfg.get("files", "lancamentos_file", fallback="lancamentos.CSV")
    plano_contas_file = cfg.get("files", "plano_contas_file", fallback="")
    if plano_contas_file == "":
        plano_contas_file = None  # None = será criado automaticamente
    
    # Carrega classificação customizada se modelo=customizado
    classificacao_customizada = None
    clas_base = None
    if modelo.lower() == "customizado":
        if not cfg.has_section("classification"):
            raise ValueError(
                "modelo=customizado requer seção [classification] no config.ini"
            )
        
        # Extrai clas_base (opcional)
        clas_base_str = cfg.get("classification", "clas_base", fallback="").strip()
        if clas_base_str:
            # Converte clas_base para TipoPlanoContas
            from pyaccount.core.account_classifier import TipoPlanoContas
            clas_base_map = {
                "CLASSIFICACAO_PADRAO_BR": TipoPlanoContas.PADRAO,
                "padrao": TipoPlanoContas.PADRAO,
                "CLASSIFICACAO_SIMPLIFICADO": TipoPlanoContas.SIMPLIFICADO,
                "simplificado": TipoPlanoContas.SIMPLIFICADO,
                "CLASSIFICACAO_IFRS": TipoPlanoContas.IFRS,
                "ifrs": TipoPlanoContas.IFRS,
            }
            clas_base = clas_base_map.get(clas_base_str, None)
            if clas_base is None:
                raise ValueError(
                    f"clas_base inválido: {clas_base_str}. "
                    f"Valores aceitos: CLASSIFICACAO_PADRAO_BR, padrao, "
                    f"CLASSIFICACAO_SIMPLIFICADO, simplificado, CLASSIFICACAO_IFRS, ifrs"
                )
        
        # Extrai todas as entradas clas_* (exceto clas_base)
        classificacao_customizada = {}
        for chave, valor in cfg.items("classification"):
            if chave.startswith("clas_") and chave != "clas_base":
                prefixo = chave.replace("clas_", "")
                classificacao_customizada[prefixo] = valor.strip()
        
        # Valida: se não houver clas_base e nenhuma entrada clas_*, gera erro
        if not clas_base and not classificacao_customizada:
            raise ValueError(
                "modelo=customizado requer pelo menos clas_base ou entradas clas_* na seção [classification]"
            )
    
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
        "agrupamento_periodo": agrupamento_periodo if agrupamento_periodo and agrupamento_periodo.lower() != "none" else None,
        "enable_query_log": enable_query_log,
        "query_log_file": query_log_file,
        "base_dir": base_dir,
        "saldos_file": saldos_file,
        "lancamentos_file": lancamentos_file,
        "plano_contas_file": plano_contas_file,
        "classificacao_customizada": classificacao_customizada,
        "clas_base": clas_base
    }
    
    # Aplica overrides se fornecidos
    config.update(overrides)
    
    return config

