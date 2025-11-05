import os
import sys
import unittest
from pathlib import Path
import pandas as pd

# Necessário para que o arquivo de testes encontre
test_file_dir = os.path.dirname(os.path.abspath(__file__))
test_dir = os.path.dirname(test_file_dir)  # test/
project_root = os.path.dirname(test_dir)  # raiz do projeto
os.chdir(test_dir)  # muda para test/ para que caminhos relativos funcionem
sys.path.insert(0, project_root)

from pyaccount import OpeningBalancesBuilder
from datetime import date, timedelta
from test.test_config import carregar_config_teste

class TestPlanoContas(unittest.TestCase):

    def test_buscar_plano_contas(self):
        """Testa recuperação e exportação do plano de contas em CSV."""
        
        # Carrega configurações do config.ini
        config = carregar_config_teste()
        
        # Converte data_inicio do config de string para date
        data_inicio = date.fromisoformat(config["data_inicio"]) if config["data_inicio"] else date(2025, 1, 1)
        
        # Calcula data para ate (dia anterior a data_inicio)
        ate = data_inicio - timedelta(days=1)
        
        # Cria o builder
        builder = OpeningBalancesBuilder(
            dsn=config["dsn"],
            user=config["user"],
            password=config["password"],
            empresa=config["empresa"],
            ate=ate,
            saida=config["saida"]
        )
        
        try:
            # Conecta ao banco de dados
            builder.db_client.connect()
            
            # Busca o plano de contas
            df_plano_contas = builder.buscar_plano_contas()
            
            # Verifica se retornou um DataFrame não vazio
            self.assertIsInstance(df_plano_contas, pd.DataFrame)
            self.assertGreater(len(df_plano_contas), 0, "Plano de contas não pode estar vazio")
            
            # Verifica se tem as colunas esperadas
            colunas_esperadas = ["CODI_EMP", "CODI_CTA", "NOME_CTA", "CLAS_CTA", 
                                 "TIPO_CTA", "SITUACAO_CTA", "BC_GROUP", "BC_NAME", "BC_ACCOUNT"]
            for col in colunas_esperadas:
                self.assertIn(col, df_plano_contas.columns, f"Coluna {col} não encontrada")
            
            # Define o caminho de saída
            out_dir = Path(config["saida"])
            out_dir.mkdir(exist_ok=True)
            out_path = out_dir / f"plano_contas_{config['empresa']}.csv"
            
            # Exporta para CSV
            df_plano_contas.to_csv(out_path, index=False, sep=";", encoding="utf-8-sig")
            
            # Verifica se o arquivo foi criado
            self.assertTrue(out_path.exists(), f"Arquivo {out_path} não foi criado")
            
            # Verifica se o arquivo tem conteúdo
            self.assertGreater(out_path.stat().st_size, 0, "Arquivo CSV está vazio")
            
            # Lê o arquivo novamente para verificar consistência
            df_verificacao = pd.read_csv(out_path, sep=";", encoding="utf-8-sig")
            self.assertEqual(len(df_verificacao), len(df_plano_contas), 
                           "Número de linhas não corresponde após leitura do CSV")
            
            print(f"\n✓ Plano de contas exportado com sucesso: {out_path.resolve()}")
            print(f"  Total de contas: {len(df_plano_contas)}")
            
        finally:
            # Sempre fecha a conexão
            builder.db_client.close()

if __name__ == '__main__':
    unittest.main()