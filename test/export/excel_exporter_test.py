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

from pyaccount import ExcelExporter, ContabilDBClient
from pyaccount.core.account_classifier import TipoPlanoContas


class TestExcelExporter(unittest.TestCase):

    def test_excel_export(self):
        """Testa geração de arquivo Excel com dados contábeis."""
        
        empresa = 267
        inicio_periodo = date(2025, 1, 1)
        fim_periodo = date(2025, 12, 31)
        
        print(f"\n--- Teste: Gerando arquivo Excel para período {inicio_periodo} a {fim_periodo} ---")
        
        # Cria cliente de banco de dados
        db_client = ContabilDBClient(
            dsn="Local_17",
            user="consulta",
            password="consulta"
        )
        
        try:
            # Conecta ao banco
            db_client.connect()
            print("✓ Conectado ao banco de dados")
            
            # Cria exportador Excel
            exporter = ExcelExporter(
                db_client=db_client,
                empresa=empresa,
                inicio=inicio_periodo,
                fim=fim_periodo,
                modelo=TipoPlanoContas.SIMPLIFICADO
            )
            
            # Exporta para Excel
            excel_path = exporter.exportar_excel(
                outdir=Path("./out"),
                nome_arquivo=f"contabilidade_{empresa}_{inicio_periodo}_{fim_periodo}.xlsx"
            )
            
            print(f"✓ Arquivo Excel gerado: {excel_path}")
            
            # Verifica se o arquivo foi criado
            self.assertTrue(excel_path.exists(), f"Arquivo {excel_path} não foi criado")
            
            # Verifica se o arquivo não está vazio
            self.assertGreater(excel_path.stat().st_size, 0, f"Arquivo {excel_path} está vazio")
            
            print(f"✓ Arquivo Excel válido (tamanho: {excel_path.stat().st_size} bytes)")
            
            # Validações básicas
            # 1. Verifica se o plano de contas foi carregado
            self.assertIsNotNone(exporter.df_pc, "Plano de contas não foi carregado")
            if exporter.df_pc is not None and not exporter.df_pc.empty:
                print(f"  - Plano de contas: {len(exporter.df_pc)} contas")
            
            # 2. Verifica se os saldos finais foram carregados
            self.assertIsNotNone(exporter.df_saldos_finais, "Saldos finais não foram carregados")
            if exporter.df_saldos_finais is not None and not exporter.df_saldos_finais.empty:
                print(f"  - Saldos finais: {len(exporter.df_saldos_finais)} contas com saldo")
            
            # 3. Verifica se as movimentações foram carregadas
            self.assertIsNotNone(exporter.df_movimentacoes, "Movimentações não foram carregadas")
            if exporter.df_movimentacoes is not None and not exporter.df_movimentacoes.empty:
                print(f"  - Movimentações: {len(exporter.df_movimentacoes)} contas")
            
            # 4. Verifica se os lançamentos foram carregados
            self.assertIsNotNone(exporter.df_lancamentos, "Lançamentos não foram carregados")
            if exporter.df_lancamentos is not None and not exporter.df_lancamentos.empty:
                print(f"  - Lançamentos: {len(exporter.df_lancamentos)} registros")
            
            # 5. Testa geração de Balanço Patrimonial
            df_bp = exporter.gerar_balanco_patrimonial()
            if not df_bp.empty:
                print(f"  - Balanço Patrimonial: {len(df_bp)} linhas")
            else:
                print(f"  - Balanço Patrimonial: vazio (sem dados)")
            
            # 6. Testa geração de DRE
            df_dre = exporter.gerar_dre()
            if not df_dre.empty:
                print(f"  - DRE: {len(df_dre)} linhas")
            else:
                print(f"  - DRE: vazio (sem dados)")
            
            print(f"\n✓ Teste concluído com sucesso!")
            
        except Exception as e:
            print(f"\n✗ Erro durante teste: {e}")
            raise
        finally:
            # Sempre fecha a conexão
            db_client.close()


if __name__ == "__main__":
    unittest.main()

