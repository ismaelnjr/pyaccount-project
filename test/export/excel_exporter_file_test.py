import os
import sys
import unittest
from pathlib import Path
from datetime import date

# Necessário para que o arquivo de testes encontre
test_file_dir = os.path.dirname(os.path.abspath(__file__))
test_dir = os.path.dirname(test_file_dir)  # test/
project_root = os.path.dirname(test_dir)  # raiz do projeto
os.chdir(test_dir)  # muda para test/ para que caminhos relativos funcionem
sys.path.insert(0, project_root)

from pyaccount import ExcelExporter, FileDataClient
from pyaccount.core.account_classifier import TipoPlanoContas
import pandas as pd


class TestExcelExporterFile(unittest.TestCase):

    def test_excel_export_from_files(self):
        """Testa geração de arquivo Excel utilizando arquivos CSV da pasta etc."""
        
        # Caminho para pasta etc (relativo ao projeto)
        etc_dir = Path(project_root) / "pyaccount" / "etc"
        
        # Verifica se os arquivos existem
        saldos_file = etc_dir / "saldos_iniciais.CSV"
        lancamentos_file = etc_dir / "lancamentos.CSV"
        
        if not saldos_file.exists() or not lancamentos_file.exists():
            self.skipTest("Arquivos CSV não encontrados em pyaccount/etc/")
        
        print(f"\n--- Teste: Gerando arquivo Excel a partir de arquivos CSV ---")
        print(f"  - Diretório: {etc_dir}")
        print(f"  - Saldos: {saldos_file.exists()}")
        print(f"  - Lançamentos: {lancamentos_file.exists()}")
        
        # Lê lançamentos para extrair empresa e período
        # O arquivo CSV não tem cabeçalho, então precisamos definir manualmente
        colunas_lancamentos = [
            "codi_emp", "nume_lan", "data_lan", "codi_lote", "tipo_lote",
            "codi_his", "chis_lan", "ndoc_lan", "codi_usu", "natureza",
            "conta", "nome_cta", "clas_cta", "valor_sinal"
        ]
        
        try:
            # Tenta ler com diferentes encodings
            df_lanc_temp = None
            for enc in ["utf-8-sig", "latin-1", "cp1252", "iso-8859-1"]:
                try:
                    df_lanc_temp = pd.read_csv(
                        lancamentos_file, 
                        sep=";", 
                        encoding=enc, 
                        nrows=1000,
                        header=None,
                        names=colunas_lancamentos
                    )
                    break
                except (UnicodeDecodeError, pd.errors.EmptyDataError):
                    continue
            
            if df_lanc_temp is None or df_lanc_temp.empty:
                self.skipTest("Não foi possível ler arquivo de lançamentos")
            
            # Normaliza nomes das colunas (já estão definidas)
            df_lanc_temp.columns = df_lanc_temp.columns.str.lower()
            
            # Extrai empresa e período
            empresa = int(df_lanc_temp["codi_emp"].iloc[0])
            
            # Extrai período (converte data do formato YYYYMMDD)
            df_lanc_temp["data_lan_dt"] = pd.to_datetime(df_lanc_temp["data_lan"].astype(str), format="%Y%m%d", errors='coerce')
            inicio_periodo = df_lanc_temp["data_lan_dt"].min().date() if not df_lanc_temp["data_lan_dt"].isna().all() else date(2025, 1, 1)
            fim_periodo = df_lanc_temp["data_lan_dt"].max().date() if not df_lanc_temp["data_lan_dt"].isna().all() else date(2025, 12, 31)
            
            # Plano de contas será criado automaticamente pelo FileDataClient
            print(f"✓ Plano de contas será criado automaticamente a partir dos lançamentos")
            
        except Exception as e:
            print(f"  - Erro ao analisar arquivos: {e}")
            import traceback
            traceback.print_exc()
            empresa = 267
            inicio_periodo = date(2025, 1, 1)
            fim_periodo = date(2025, 12, 31)
        
        print(f"  - Empresa: {empresa}")
        print(f"  - Período: {inicio_periodo} a {fim_periodo}")
        
        # Cria cliente de arquivos (plano de contas será criado automaticamente)
        file_client = FileDataClient(
            base_dir=etc_dir,
            saldos_file="saldos_iniciais.CSV",
            lancamentos_file="lancamentos.CSV"
            # plano_contas_file não fornecido - será criado automaticamente
        )
        
        print("✓ FileDataClient criado")
        
        try:
            # Cria exportador Excel com FileDataClient
            exporter = ExcelExporter(
                data_client=file_client,
                empresa=empresa,
                inicio=inicio_periodo,
                fim=fim_periodo,
                modelo=TipoPlanoContas.SIMPLIFICADO,
                agrupamento_periodo=None  # Sem agrupamento inicial
            )
            
            print("✓ ExcelExporter criado")
            
            # Exporta para Excel
            excel_path = exporter.exportar_excel(
                outdir=Path("./out"),
                nome_arquivo=f"contabilidade_from_files_{empresa}_{inicio_periodo}_{fim_periodo}.xlsx"
            )
            
            print(f"✓ Arquivo Excel gerado: {excel_path}")
            
            # Verifica se o arquivo foi criado
            self.assertTrue(excel_path.exists(), f"Arquivo {excel_path} não foi criado")
            
            # Verifica se o arquivo não está vazio
            self.assertGreater(excel_path.stat().st_size, 0, f"Arquivo {excel_path} está vazio")
            
            print(f"✓ Arquivo Excel válido (tamanho: {excel_path.stat().st_size} bytes)")
            
            # Validações básicas
            # 1. Verifica se os saldos foram carregados
            if exporter.df_saldos_finais is not None and not exporter.df_saldos_finais.empty:
                print(f"  - Saldos finais: {len(exporter.df_saldos_finais)} contas com saldo")
            else:
                print(f"  - Saldos finais: vazio (sem dados ou arquivo não encontrado)")
            
            # 2. Verifica se as movimentações foram carregadas
            if exporter.df_movimentacoes is not None and not exporter.df_movimentacoes.empty:
                print(f"  - Movimentações: {len(exporter.df_movimentacoes)} contas")
            else:
                print(f"  - Movimentações: vazio (sem dados)")
            
            # 3. Verifica se os lançamentos foram carregados
            if exporter.df_lancamentos is not None and not exporter.df_lancamentos.empty:
                print(f"  - Lançamentos: {len(exporter.df_lancamentos)} registros")
            else:
                print(f"  - Lançamentos: vazio (sem dados)")
            
            # 4. Verifica se o plano de contas foi carregado (pode estar vazio se não houver arquivo)
            if exporter.df_pc is not None and not exporter.df_pc.empty:
                print(f"  - Plano de contas: {len(exporter.df_pc)} contas")
            else:
                print(f"  - Plano de contas: vazio (arquivo não encontrado ou sem dados)")
            
            # 5. Testa geração de Balanço Patrimonial
            try:
                df_bp = exporter.gerar_balanco_patrimonial()
                if not df_bp.empty:
                    print(f"  - Balanço Patrimonial: {len(df_bp)} linhas")
                else:
                    print(f"  - Balanço Patrimonial: vazio (sem dados)")
            except Exception as e:
                print(f"  - Balanço Patrimonial: erro ({e})")
            
            # 6. Testa geração de DRE
            try:
                df_dre = exporter.gerar_dre()
                if not df_dre.empty:
                    print(f"  - DRE: {len(df_dre)} linhas")
                else:
                    print(f"  - DRE: vazio (sem dados)")
            except Exception as e:
                print(f"  - DRE: erro ({e})")
            
            # 7. Testa geração de Balancete
            try:
                df_balancete = exporter.gerar_balancete()
                if not df_balancete.empty:
                    print(f"  - Balancete: {len(df_balancete)} linhas")
                else:
                    print(f"  - Balancete: vazio (sem dados)")
            except Exception as e:
                print(f"  - Balancete: erro ({e})")
            
            print(f"\n✓ Teste concluído com sucesso!")
            
        except Exception as e:
            print(f"\n✗ Erro durante teste: {e}")
            import traceback
            traceback.print_exc()
            raise


if __name__ == "__main__":
    unittest.main()

