import os
import sys
import unittest
import pandas as pd
from pathlib import Path

# Necessário para que o arquivo de testes encontre
test_file_dir = os.path.dirname(os.path.abspath(__file__))
test_dir = os.path.dirname(test_file_dir)  # test/
project_root = os.path.dirname(test_dir)  # raiz do projeto
os.chdir(test_dir)  # muda para test/ para que caminhos relativos funcionem
sys.path.insert(0, project_root)

from pyaccount import OpeningBalancesBuilder
from pyaccount.builders.opening_balances import carregar_saldos_iniciais_de_arquivo
from datetime import date, timedelta
from test.test_config import carregar_config_teste

class TestBuildOpeningBalances(unittest.TestCase):

    def test_build_opening_balances(self):
        """Testa geração de saldos usando saldos de abertura baseados em data_inicio do config."""
        
        # Carrega configurações do config.ini
        config = carregar_config_teste()
        
        # Converte datas do config de string para date
        data_inicio = date.fromisoformat(config["data_inicio"]) if config["data_inicio"] else date(2025, 1, 1)
        data_fim = date.fromisoformat(config["data_fim"]) if config["data_fim"] else date(2025, 12, 31)
        
        # Calcula data de abertura (dia anterior a data_inicio)
        ate_abertura = data_inicio - timedelta(days=1)
        
        # Passo 1: Gera saldos iniciais no dia anterior a data_inicio
        print(f"\n--- Passo 1: Gerando saldos de abertura em {ate_abertura} ---")
        builder_2023 = OpeningBalancesBuilder(
            dsn=config["dsn"],
            user=config["user"],
            password=config["password"],
            empresa=config["empresa"],
            ate=ate_abertura,
            saida=config["saida"]
        )
        out_path_2023 = builder_2023.execute()
        print(f"✓ Saldos de abertura gerados: {out_path_2023}")
        
        # Verifica se o arquivo foi criado
        self.assertTrue(out_path_2023.exists(), f"Arquivo {out_path_2023} não foi criado")
        
        # Passo 2: Carrega os saldos de abertura como saldos iniciais
        print(f"\n--- Passo 2: Carregando saldos de abertura para cálculo até {data_fim} ---")
        saldos_abertura = carregar_saldos_iniciais_de_arquivo(out_path_2023)
        print(f"✓ Carregados {len(saldos_abertura)} contas com saldo de abertura")
        
        # Verifica se há saldos carregados
        self.assertGreater(len(saldos_abertura), 0, "Nenhum saldo de abertura foi carregado")
        
        # Passo 3: Gera saldos finais usando saldos de abertura + movimentações
        print(f"\n--- Passo 3: Gerando saldos finais até {data_fim} (abertura + movimentações) ---")
        builder_2024 = OpeningBalancesBuilder(
            dsn=config["dsn"],
            user=config["user"],
            password=config["password"],
            empresa=config["empresa"],
            ate=data_fim,
            saida=config["saida"],
            saldos_iniciais=saldos_abertura,
            data_abertura=ate_abertura
        )
        out_path_2024 = builder_2024.execute()
        print(f"✓ Saldos finais gerados: {out_path_2024}")
        
        # Verifica se o arquivo foi criado
        self.assertTrue(out_path_2024.exists(), f"Arquivo {out_path_2024} não foi criado")
        
        # Verifica se o arquivo tem conteúdo
        self.assertGreater(out_path_2024.stat().st_size, 0, "Arquivo CSV está vazio")
        
        # Passo 4: Validação adicional - lê o arquivo gerado
        print("\n--- Passo 4: Validando arquivo gerado ---")
        df_resultado = pd.read_csv(out_path_2024, sep=";", encoding="utf-8-sig")
        print(f"✓ Arquivo contém {len(df_resultado)} contas com saldo")
        
        # Verifica colunas esperadas
        colunas_esperadas = ["conta", "NOME_CTA", "BC_GROUP", "saldo", "CLAS_CTA", "BC_ACCOUNT", "empresa", "data_corte"]
        for col in colunas_esperadas:
            self.assertIn(col, df_resultado.columns, f"Coluna {col} não encontrada no resultado")
        
        # Verifica que data_corte está correta
        data_fim_str = data_fim.strftime("%Y-%m-%d")
        self.assertEqual(df_resultado["data_corte"].iloc[0], data_fim_str, 
                        f"Data de corte no resultado deve ser {data_fim_str}")
        
        print(f"\n✓ Teste concluído com sucesso!")
        print(f"  - Saldos de abertura: {out_path_2023}")
        print(f"  - Saldos finais: {out_path_2024}")

if __name__ == '__main__':
    unittest.main()