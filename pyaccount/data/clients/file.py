#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cliente para leitura de dados contábeis a partir de arquivos CSV.

Esta classe permite ler dados contábeis de arquivos CSV exportados do sistema,
permitindo processamento offline sem necessidade de conexão com banco de dados.
"""
from datetime import date
from pathlib import Path
from typing import Optional
import pandas as pd


from pyaccount.data.client import DataClient


class FileDataClient(DataClient):
    """
    Cliente para ler dados contábeis de arquivos CSV.
    
    Esta classe lê dados de arquivos CSV exportados do sistema contábil,
    permitindo processamento offline sem necessidade de conexão com banco de dados.
    """
    
    def __init__(
        self,
        base_dir: Path,
        saldos_file: Optional[str] = None,
        lancamentos_file: Optional[str] = None,
        plano_contas_file: Optional[str] = None
    ):
        """
        Inicializa o cliente de arquivos.
        
        Args:
            base_dir: Diretório base onde os arquivos CSV estão localizados
            saldos_file: Nome do arquivo de saldos (padrão: "saldos_iniciais.CSV")
            lancamentos_file: Nome do arquivo de lançamentos (padrão: "lancamentos.CSV")
            plano_contas_file: Nome do arquivo de plano de contas (opcional, se None será criado automaticamente a partir dos lançamentos)
        """
        self.base_dir = Path(base_dir)
        self.saldos_file = saldos_file or "saldos_iniciais.CSV"
        self.lancamentos_file = lancamentos_file or "lancamentos.CSV"
        self.plano_contas_file = plano_contas_file  # None quando não fornecido (cria automaticamente)
        
        # Cache para arquivos carregados
        self._df_saldos: Optional[pd.DataFrame] = None
        self._df_lancamentos: Optional[pd.DataFrame] = None
        self._df_plano_contas: Optional[pd.DataFrame] = None
    
    def _ler_csv(self, arquivo: str, separador: str = ";", encoding: str = "utf-8-sig") -> pd.DataFrame:
        """
        Lê um arquivo CSV com tratamento de encoding e separador.
        
        Args:
            arquivo: Nome do arquivo (relativo a base_dir)
            separador: Separador de campos (padrão: ";")
            encoding: Encoding do arquivo (padrão: "utf-8-sig")
            
        Returns:
            DataFrame com os dados do arquivo
            
        Raises:
            FileNotFoundError: Se o arquivo não existir
        """
        caminho = self.base_dir / arquivo
        if not caminho.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")
        
        # Tenta ler com encoding padrão, se falhar tenta outros
        try:
            df = pd.read_csv(caminho, sep=separador, encoding=encoding)
        except UnicodeDecodeError:
            # Tenta outros encodings comuns
            for enc in ["latin-1", "cp1252", "iso-8859-1"]:
                try:
                    df = pd.read_csv(caminho, sep=separador, encoding=enc)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise ValueError(f"Não foi possível decodificar o arquivo {caminho}")
        
        return df
    
    def _converter_valor_numerico(self, serie: pd.Series) -> pd.Series:
        """
        Converte série de valores numéricos de vírgula para ponto decimal.
        
        Args:
            serie: Série com valores como string (ex: "19588,15")
            
        Returns:
            Série com valores numéricos convertidos
        """
        if serie.dtype == 'object':
            # Converte vírgula para ponto e depois para numérico
            serie = serie.astype(str).str.replace(",", ".", regex=False)
        
        return pd.to_numeric(serie, errors='coerce')
    
    def _converter_data(self, serie: pd.Series) -> pd.Series:
        """
        Converte série de datas de formato YYYYMMDD ou YYYY-MM-DD para date.
        
        Args:
            serie: Série com datas como string (ex: "20241231" ou "2024-12-31")
            
        Returns:
            Série com objetos date
        """
        # Tenta converter como string primeiro
        serie_str = serie.astype(str)
        
        # Detecta formato YYYYMMDD (8 dígitos)
        if serie_str.str.len().eq(8).all():
            # Formato YYYYMMDD
            serie_dt = pd.to_datetime(serie_str, format="%Y%m%d", errors='coerce')
        else:
            # Tenta formato YYYY-MM-DD ou outros
            serie_dt = pd.to_datetime(serie_str, errors='coerce')
        
        return serie_dt.dt.date
    
    def buscar_plano_contas(self, empresa: int) -> pd.DataFrame:
        """
        Busca plano de contas do arquivo CSV ou cria automaticamente a partir dos lançamentos.
        
        Args:
            empresa: Código da empresa (filtra por CODI_EMP se disponível)
            
        Returns:
            DataFrame com colunas: CODI_EMP, CODI_CTA, NOME_CTA, CLAS_CTA, 
                                  TIPO_CTA, DATA_CTA, SITUACAO_CTA
        """
        if self._df_plano_contas is None:
            if self.plano_contas_file is not None:
                # Lê arquivo de plano de contas fornecido (sem cabeçalho)
                colunas_plano_contas = [
                    "CODI_EMP", "CODI_CTA", "NOME_CTA", "CLAS_CTA",
                    "TIPO_CTA", "DATA_CTA", "SITUACAO_CTA"
                ]
                
                caminho = self.base_dir / self.plano_contas_file
                df_temp = None
                ultimo_erro = None
                
                for enc in ["utf-8-sig", "latin-1", "cp1252", "iso-8859-1"]:
                    try:
                        df_temp = pd.read_csv(
                            caminho,
                            sep=";",
                            encoding=enc,
                            header=None,
                            names=colunas_plano_contas
                        )
                        if df_temp is not None and not df_temp.empty:
                            break
                    except Exception as e:
                        ultimo_erro = e
                        continue
                
                if df_temp is None or df_temp.empty:
                    raise ValueError(f"Não foi possível ler o arquivo de plano de contas: {caminho}")
                
                self._df_plano_contas = df_temp
                
                # Normaliza nomes das colunas para maiúsculas
                self._df_plano_contas.columns = self._df_plano_contas.columns.str.upper()
                
                # Converte DATA_CTA
                if "DATA_CTA" in self._df_plano_contas.columns:
                    self._df_plano_contas["DATA_CTA"] = self._converter_data(self._df_plano_contas["DATA_CTA"])
            else:
                # Cria plano de contas automaticamente a partir dos lançamentos
                self._df_plano_contas = self._criar_plano_contas_dos_lancamentos(empresa)
        
        df = self._df_plano_contas.copy()
        
        # Filtra por empresa se a coluna existir
        if "CODI_EMP" in df.columns:
            df = df[df["CODI_EMP"].astype(int) == empresa]
        
        return df
    
    def buscar_saldos(self, empresa: int, ate: date) -> pd.DataFrame:
        """
        Busca saldos do arquivo CSV até uma data de corte.
        
        Args:
            empresa: Código da empresa (não usado para arquivos CSV)
            ate: Data de corte (filtra saldos até esta data)
            
        Returns:
            DataFrame com colunas: conta, saldo
        """
        if self._df_saldos is None:
            # Arquivo de saldos: conta;saldo;data_do_saldo (sem cabeçalho)
            colunas_saldos = ["conta", "saldo", "data_do_saldo"]
            
            # Tenta ler sem cabeçalho com diferentes encodings
            caminho = self.base_dir / self.saldos_file
            df_temp = None
            
            for enc in ["utf-8-sig", "latin-1", "cp1252", "iso-8859-1"]:
                try:
                    df_temp = pd.read_csv(
                        caminho,
                        sep=";",
                        encoding=enc,
                        header=None,
                        names=colunas_saldos
                    )
                    if not df_temp.empty:
                        break
                except (UnicodeDecodeError, pd.errors.EmptyDataError, Exception):
                    continue
            
            if df_temp is None or df_temp.empty:
                raise ValueError(f"Não foi possível ler o arquivo de saldos: {caminho}")
            
            self._df_saldos = df_temp
            
            # Normaliza nomes das colunas para minúsculas
            self._df_saldos.columns = self._df_saldos.columns.str.lower()
            
            # Converte tipos
            if "conta" in self._df_saldos.columns:
                self._df_saldos["conta"] = self._df_saldos["conta"].astype(str)
            
            if "saldo" in self._df_saldos.columns:
                self._df_saldos["saldo"] = self._converter_valor_numerico(self._df_saldos["saldo"])
            
            # Converte data se existir coluna de data
            if "data_do_saldo" in self._df_saldos.columns:
                self._df_saldos["data_do_saldo"] = self._converter_data(self._df_saldos["data_do_saldo"])
        
        df = self._df_saldos.copy()
        
        # Filtra por data se coluna de data existir
        if "data_do_saldo" in df.columns:
            df = df[df["data_do_saldo"] <= ate]
        elif "data" in df.columns:
            df = df[df["data"] <= ate]
        
        # Retorna apenas conta e saldo
        if "conta" in df.columns and "saldo" in df.columns:
            return df[["conta", "saldo"]].copy()
        else:
            # Se não tiver as colunas esperadas, assume que são as primeiras duas colunas
            df.columns = ["conta", "saldo"] + list(df.columns[2:])
            return df[["conta", "saldo"]].copy()
    
    def buscar_lancamentos_periodo(self, empresa: int, inicio: date, fim: date) -> pd.DataFrame:
        """
        Busca lançamentos do arquivo CSV de um período específico.
        
        Args:
            empresa: Código da empresa (filtra por codi_emp se disponível)
            inicio: Data inicial do período (inclusive)
            fim: Data final do período (inclusive)
            
        Returns:
            DataFrame com colunas normalizadas: codi_emp, nume_lan, data_lan, vlor_lan,
                                  cdeb_lan, ccre_lan, codi_his, chis_lan,
                                  ndoc_lan, codi_lote, tipo, codi_usu,
                                  orig_lan, origem_descricao
        """
        if self._df_lancamentos is None:
            # Define colunas esperadas (arquivo CSV não tem cabeçalho)
            colunas_lancamentos = [
                "codi_emp", "nume_lan", "data_lan", "codi_lote", "tipo_lote",
                "codi_his", "chis_lan", "ndoc_lan", "codi_usu", "natureza",
                "conta", "nome_cta", "clas_cta", "valor_sinal"
            ]
            
            # Tenta ler sem cabeçalho com diferentes encodings
            caminho = self.base_dir / self.lancamentos_file
            df_temp = None
            ultimo_erro = None
            
            for enc in ["utf-8-sig", "latin-1", "cp1252", "iso-8859-1"]:
                try:
                    df_temp = pd.read_csv(
                        caminho,
                        sep=";",
                        encoding=enc,
                        header=None,
                        names=colunas_lancamentos,
                        on_bad_lines='skip',  # Ignora linhas com número diferente de campos
                        engine='python'  # Usa engine Python para maior flexibilidade
                    )
                    if df_temp is not None and not df_temp.empty:
                        break
                except Exception as e:
                    ultimo_erro = e
                    continue
            
            if df_temp is None or df_temp.empty:
                erro_msg = f"Não foi possível ler o arquivo de lançamentos: {caminho}"
                if ultimo_erro:
                    erro_msg += f" (último erro: {ultimo_erro})"
                raise ValueError(erro_msg)
            
            self._df_lancamentos = df_temp
            
            # Normaliza nomes das colunas para minúsculas
            self._df_lancamentos.columns = self._df_lancamentos.columns.str.lower()
            
            # Converte tipos básicos
            if "conta" in self._df_lancamentos.columns:
                self._df_lancamentos["conta"] = self._df_lancamentos["conta"].astype(str)
            
            # Converte data_lan
            if "data_lan" in self._df_lancamentos.columns:
                self._df_lancamentos["data_lan"] = self._converter_data(self._df_lancamentos["data_lan"])
            
            # Converte valor_sinal se existir (pode ser valor com sinal já aplicado)
            if "valor_sinal" in self._df_lancamentos.columns:
                self._df_lancamentos["valor_sinal"] = self._converter_valor_numerico(self._df_lancamentos["valor_sinal"])
        
        df = self._df_lancamentos.copy()
        
        # Filtra por empresa se coluna existir
        if "codi_emp" in df.columns:
            df = df[df["codi_emp"].astype(int) == empresa]
        
        # Filtra por período
        if "data_lan" in df.columns:
            df = df[(df["data_lan"] >= inicio) & (df["data_lan"] <= fim)]
        
        # Mapeia colunas do CSV para formato esperado
        # CSV tem: codi_emp, nume_lan, data_lan, codi_lote, tipo_lote, codi_his, chis_lan, 
        #          ndoc_lan, codi_usu, natureza, conta, nome_cta, clas_cta, valor_sinal
        # Cada linha do CSV representa um movimento (D ou C), não um lançamento completo
        
        # Cria DataFrame base com colunas comuns
        df_result = pd.DataFrame()
        
        # Copia colunas que existem diretamente
        colunas_diretas = ["codi_emp", "nume_lan", "data_lan", "codi_his", "chis_lan", 
                          "ndoc_lan", "codi_lote", "codi_usu"]
        for col in colunas_diretas:
            if col in df.columns:
                df_result[col] = df[col]
        
        # Mapeia tipo_lote para tipo
        if "tipo_lote" in df.columns:
            df_result["tipo"] = df["tipo_lote"]
        else:
            df_result["tipo"] = None
        
        # Converte valor_sinal para vlor_lan (valor sempre positivo)
        if "valor_sinal" in df.columns:
            df_result["vlor_lan"] = self._converter_valor_numerico(df["valor_sinal"]).abs()
        else:
            df_result["vlor_lan"] = 0.0
        
        # Preenche cdeb_lan e ccre_lan baseado em natureza
        if "natureza" in df.columns and "conta" in df.columns:
            df_result["cdeb_lan"] = df.apply(
                lambda row: str(row["conta"]) if str(row.get("natureza", "")).upper() == "D" else "",
                axis=1
            )
            df_result["ccre_lan"] = df.apply(
                lambda row: str(row["conta"]) if str(row.get("natureza", "")).upper() == "C" else "",
                axis=1
            )
        else:
            df_result["cdeb_lan"] = ""
            df_result["ccre_lan"] = ""
        
        # Colunas não disponíveis no CSV
        df_result["orig_lan"] = None
        df_result["origem_descricao"] = None
        
        # Ordena por data, lote e número do lançamento
        if "data_lan" in df_result.columns:
            sort_cols = ["data_lan"]
            if "codi_lote" in df_result.columns:
                sort_cols.append("codi_lote")
            if "nume_lan" in df_result.columns:
                sort_cols.append("nume_lan")
            df_result = df_result.sort_values(sort_cols)
        
        return df_result
    
    def buscar_movimentacoes_periodo(self, empresa: int, de: date, ate: date) -> pd.DataFrame:
        """
        Busca movimentações calculadas a partir dos lançamentos do período.
        
        Args:
            empresa: Código da empresa
            de: Data inicial do período (exclusiva - movimentações após esta data)
            ate: Data final do período (inclusive - movimentações até esta data)
            
        Returns:
            DataFrame com colunas: conta, movimento
            movimento = débitos - créditos (valor positivo aumenta saldo, negativo diminui)
        """
        # Busca lançamentos do período (buscar_lancamentos_periodo usa inicio inclusive)
        # Para buscar_movimentacoes_periodo, de é exclusivo, então busca de de+1 até ate
        from datetime import timedelta
        if de:
            inicio = de + timedelta(days=1)
        else:
            # Se de não foi fornecido, busca desde o início (data mínima)
            inicio = date(1900, 1, 1)
        df_lanc = self.buscar_lancamentos_periodo(empresa, inicio, ate)
        
        if df_lanc.empty:
            return pd.DataFrame(columns=["conta", "movimento"])
        
        # Calcula movimentações a partir dos lançamentos
        movimentos = {}
        
        for _, row in df_lanc.iterrows():
            # Débito
            if pd.notna(row.get("cdeb_lan")) and str(row.get("cdeb_lan")).strip():
                conta = str(row["cdeb_lan"]).strip()
                valor = float(row.get("vlor_lan", 0)) if pd.notna(row.get("vlor_lan")) else 0
                if conta not in movimentos:
                    movimentos[conta] = 0
                movimentos[conta] += valor
            
            # Crédito
            if pd.notna(row.get("ccre_lan")) and str(row.get("ccre_lan")).strip():
                conta = str(row["ccre_lan"]).strip()
                valor = float(row.get("vlor_lan", 0)) if pd.notna(row.get("vlor_lan")) else 0
                if conta not in movimentos:
                    movimentos[conta] = 0
                movimentos[conta] -= valor
        
        # Cria DataFrame
        if movimentos:
            df_result = pd.DataFrame([
                {"conta": str(conta), "movimento": movimento}
                for conta, movimento in movimentos.items()
                if movimento != 0
            ])
            df_result["conta"] = df_result["conta"].astype(str)
            df_result = df_result.sort_values("conta")
        else:
            df_result = pd.DataFrame(columns=["conta", "movimento"])
        
        return df_result
    
    def _criar_plano_contas_dos_lancamentos(self, empresa: int) -> pd.DataFrame:
        """
        Cria plano de contas automaticamente a partir dos lançamentos.
        
        Args:
            empresa: Código da empresa
            
        Returns:
            DataFrame com colunas: CODI_EMP, CODI_CTA, NOME_CTA, CLAS_CTA,
                                  TIPO_CTA, DATA_CTA, SITUACAO_CTA
        """
        # Garante que os lançamentos foram carregados
        # Usa uma data ampla para buscar todos os lançamentos
        from datetime import date
        df_lanc = self.buscar_lancamentos_periodo(empresa, date(1900, 1, 1), date(2100, 12, 31))
        
        if df_lanc.empty:
            # Se não houver lançamentos, retorna DataFrame vazio com as colunas corretas
            return pd.DataFrame(columns=[
                "CODI_EMP", "CODI_CTA", "NOME_CTA", "CLAS_CTA",
                "TIPO_CTA", "DATA_CTA", "SITUACAO_CTA"
            ])
        
        # Carrega lançamentos originais para ter acesso a nome_cta e clas_cta
        if self._df_lancamentos is None:
            # Força carregamento dos lançamentos
            _ = self.buscar_lancamentos_periodo(empresa, date(1900, 1, 1), date(2100, 12, 31))
        
        # Usa o DataFrame original para ter acesso a todas as colunas
        if self._df_lancamentos is None or self._df_lancamentos.empty:
            return pd.DataFrame(columns=[
                "CODI_EMP", "CODI_CTA", "NOME_CTA", "CLAS_CTA",
                "TIPO_CTA", "DATA_CTA", "SITUACAO_CTA"
            ])
        
        df_orig = self._df_lancamentos.copy()
        
        # Filtra por empresa se coluna existir
        if "codi_emp" in df_orig.columns:
            df_orig = df_orig[df_orig["codi_emp"].astype(int) == empresa]
        
        # Extrai contas únicas com informações completas
        # Agrupa por conta para obter nome_cta, clas_cta e data mínima
        plano_contas_list = []
        
        if "conta" in df_orig.columns and "nome_cta" in df_orig.columns and "clas_cta" in df_orig.columns:
            # Agrupa por conta
            for conta, grupo in df_orig.groupby("conta"):
                # Pega informações da primeira linha do grupo
                primeira_linha = grupo.iloc[0]
                
                # Extrai informações
                codi_emp = int(primeira_linha.get("codi_emp", empresa)) if pd.notna(primeira_linha.get("codi_emp")) else empresa
                codi_cta = str(conta)
                nome_cta = str(primeira_linha.get("nome_cta", ""))
                clas_cta = str(primeira_linha.get("clas_cta", ""))
                tipo_cta = "A"  # Sempre analítica nos lançamentos
                data_cta = grupo["data_lan"].min() if "data_lan" in grupo.columns and not grupo["data_lan"].isna().all() else date(1900, 1, 1)
                situacao_cta = "A"  # Sempre ativa
                
                plano_contas_list.append({
                    "CODI_EMP": codi_emp,
                    "CODI_CTA": codi_cta,
                    "NOME_CTA": nome_cta,
                    "CLAS_CTA": clas_cta,
                    "TIPO_CTA": tipo_cta,
                    "DATA_CTA": data_cta,
                    "SITUACAO_CTA": situacao_cta
                })
        
        # Cria DataFrame
        if plano_contas_list:
            df_plano = pd.DataFrame(plano_contas_list)
            # Ordena por CODI_CTA
            df_plano = df_plano.sort_values("CODI_CTA")
        else:
            df_plano = pd.DataFrame(columns=[
                "CODI_EMP", "CODI_CTA", "NOME_CTA", "CLAS_CTA",
                "TIPO_CTA", "DATA_CTA", "SITUACAO_CTA"
            ])
        
        return df_plano

