import os
import sys
import unittest
import tempfile
import configparser
from pathlib import Path

# Necessário para que o arquivo de testes encontre
test_file_dir = os.path.dirname(os.path.abspath(__file__))
test_dir = os.path.dirname(test_file_dir)  # test/
project_root = os.path.dirname(test_dir)  # raiz do projeto
os.chdir(test_dir)  # muda para test/ para que caminhos relativos funcionem
sys.path.insert(0, project_root)

from pyaccount.core.account_classifier import (
    AccountClassifier,
    obter_classificacao_do_modelo,
    TipoPlanoContas,
    CLASSIFICACAO_PADRAO_BR,
    CLASSIFICACAO_SIMPLIFICADO,
    CLASSIFICACAO_IFRS
)


class TestAccountClassifier(unittest.TestCase):
    """Testes para a classe AccountClassifier."""

    def test_init_com_mapeamento_padrao(self):
        """Testa inicialização com mapeamento padrão."""
        classifier = AccountClassifier()
        
        self.assertIsNotNone(classifier.mapeamento)
        self.assertEqual(classifier.mapeamento, CLASSIFICACAO_PADRAO_BR)
        self.assertGreater(len(classifier.prefixos), 0)
        # Verifica que prefixos estão ordenados por comprimento (maior primeiro)
        prefixos = classifier.prefixos
        self.assertTrue(all(len(prefixos[i]) >= len(prefixos[i+1]) 
                           for i in range(len(prefixos)-1)))

    def test_init_com_mapeamento_customizado(self):
        """Testa inicialização com mapeamento customizado."""
        mapeamento_custom = {
            "1": "Assets:Customizado",
            "11": "Assets:Ativo-Circulante-Custom",
            "2": "Liabilities:Custom"
        }
        classifier = AccountClassifier(mapeamento_custom)
        
        self.assertEqual(classifier.mapeamento, mapeamento_custom)
        self.assertEqual(len(classifier.prefixos), 3)
        # Verifica ordem: prefixos mais longos primeiro
        self.assertEqual(classifier.prefixos[0], "11")
        self.assertEqual(classifier.prefixos[1], "1")
        self.assertEqual(classifier.prefixos[2], "2")

    def test_classificar_com_prefixos_especificos(self):
        """Testa classificação priorizando prefixos mais específicos."""
        classifier = AccountClassifier()
        
        # Testa que "11" (mais específico) tem prioridade sobre "1"
        resultado = classifier.classificar("11210100708")
        self.assertEqual(resultado, "Assets:Ativo-Circulante")
        
        # Testa que "31" tem prioridade sobre "3"
        resultado = classifier.classificar("311203")
        self.assertEqual(resultado, "Expenses:Custos")
        
        # Testa classificação genérica
        resultado = classifier.classificar("4")
        self.assertEqual(resultado, "Income:Receitas")

    def test_classificar_casos_especificos(self):
        """Testa casos específicos de classificação."""
        classifier = AccountClassifier()
        
        # Ativo Circulante
        self.assertEqual(classifier.classificar("11210100708"), "Assets:Ativo-Circulante")
        self.assertEqual(classifier.classificar("11"), "Assets:Ativo-Circulante")
        
        # Ativo Não Circulante
        self.assertEqual(classifier.classificar("121"), "Assets:Ativo-Nao-Circulante")
        
        # Passivo Circulante
        self.assertEqual(classifier.classificar("211"), "Liabilities:Passivo-Circulante")
        
        # Patrimônio Líquido
        self.assertEqual(classifier.classificar("231"), "Equity:Patrimonio-Liquido")
        
        # Custos
        self.assertEqual(classifier.classificar("311"), "Expenses:Custos")
        
        # Receitas Operacionais
        self.assertEqual(classifier.classificar("411"), "Income:Receitas-Operacionais")
        
        # Contas de Compensação
        self.assertEqual(classifier.classificar("9"), "Equity:Contas-Compensacao")

    def test_classificar_valores_vazios(self):
        """Testa classificação com valores vazios ou None."""
        classifier = AccountClassifier()
        
        self.assertEqual(classifier.classificar(""), "Unknown")
        self.assertEqual(classifier.classificar(None), "Unknown")
        self.assertEqual(classifier.classificar("   "), "Unknown")

    def test_classificar_nao_mapeado(self):
        """Testa classificação de CLAS_CTA não mapeada."""
        classifier = AccountClassifier()
        
        # CLAS_CTA que não começa com nenhum prefixo conhecido
        # (não usa "9" pois está mapeado para Contas-Compensacao)
        self.assertEqual(classifier.classificar("888"), "Unknown")
        self.assertEqual(classifier.classificar("777"), "Unknown")
        self.assertEqual(classifier.classificar("0"), "Unknown")

    def test_classificar_ignora_tipo_cta(self):
        """Testa que tipo_cta não afeta a classificação."""
        classifier = AccountClassifier()
        
        resultado1 = classifier.classificar("11", "A")
        resultado2 = classifier.classificar("11", "S")
        resultado3 = classifier.classificar("11", None)
        
        self.assertEqual(resultado1, resultado2)
        self.assertEqual(resultado2, resultado3)
        self.assertEqual(resultado1, "Assets:Ativo-Circulante")

    def test_carregar_do_config(self):
        """Testa carregamento de configuração de um dicionário."""
        config = {
            "clas_1": "Assets:Customizado",
            "clas_11": "Assets:Ativo-Circulante-Custom",
            "clas_2": "Liabilities:Custom",
            "outra_chave": "valor_ignorado"
        }
        
        classifier = AccountClassifier.carregar_do_config(config)
        
        self.assertIsNotNone(classifier)
        self.assertEqual(classifier.classificar("1"), "Assets:Customizado")
        self.assertEqual(classifier.classificar("11"), "Assets:Ativo-Circulante-Custom")
        self.assertEqual(classifier.classificar("2"), "Liabilities:Custom")

    def test_carregar_do_config_vazio(self):
        """Testa carregamento de configuração vazia."""
        config = {}
        classifier = AccountClassifier.carregar_do_config(config)
        self.assertIsNone(classifier)
        
        config = {"outra_chave": "valor"}
        classifier = AccountClassifier.carregar_do_config(config)
        self.assertIsNone(classifier)

    def test_carregar_do_ini(self):
        """Testa carregamento de configuração de arquivo INI."""
        # Cria arquivo INI temporário
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            config_path = f.name
            f.write("""[classification]
clas_1 = Assets:Customizado
clas_11 = Assets:Ativo-Circulante-Custom
clas_2 = Liabilities:Custom
""")
        
        try:
            classifier = AccountClassifier.carregar_do_ini(config_path)
            
            self.assertIsNotNone(classifier)
            self.assertEqual(classifier.classificar("1"), "Assets:Customizado")
            self.assertEqual(classifier.classificar("11"), "Assets:Ativo-Circulante-Custom")
            self.assertEqual(classifier.classificar("2"), "Liabilities:Custom")
        finally:
            # Remove arquivo temporário
            Path(config_path).unlink()

    def test_carregar_do_ini_secao_inexistente(self):
        """Testa carregamento de configuração com seção inexistente."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            config_path = f.name
            f.write("""[database]
dsn = SQLANYWHERE17
""")
        
        try:
            classifier = AccountClassifier.carregar_do_ini(config_path, "classification")
            self.assertIsNone(classifier)
        finally:
            Path(config_path).unlink()

    def test_carregar_do_ini_secao_customizada(self):
        """Testa carregamento de configuração com seção customizada."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            config_path = f.name
            f.write("""[custom_classification]
clas_1 = Assets:Custom
clas_2 = Liabilities:Custom
""")
        
        try:
            classifier = AccountClassifier.carregar_do_ini(config_path, "custom_classification")
            
            self.assertIsNotNone(classifier)
            self.assertEqual(classifier.classificar("1"), "Assets:Custom")
            self.assertEqual(classifier.classificar("2"), "Liabilities:Custom")
        finally:
            Path(config_path).unlink()

    def test_carregar_do_ini_vazio(self):
        """Testa carregamento de arquivo INI sem configuração de classificação."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            config_path = f.name
            f.write("""[classification]
outra_chave = valor
""")
        
        try:
            classifier = AccountClassifier.carregar_do_ini(config_path)
            self.assertIsNone(classifier)
        finally:
            Path(config_path).unlink()

    def test_modelo_padrao(self):
        """Testa uso do modelo padrão."""
        classificacao = obter_classificacao_do_modelo(TipoPlanoContas.PADRAO)
        classifier = AccountClassifier(classificacao)
        
        self.assertEqual(classifier.mapeamento, CLASSIFICACAO_PADRAO_BR)
        self.assertEqual(classifier.classificar("11"), "Assets:Ativo-Circulante")
        self.assertEqual(classifier.classificar("31"), "Expenses:Custos")

    def test_modelo_simplificado(self):
        """Testa uso do modelo simplificado."""
        classificacao = obter_classificacao_do_modelo(TipoPlanoContas.SIMPLIFICADO)
        classifier = AccountClassifier(classificacao)
        
        self.assertEqual(classifier.mapeamento, CLASSIFICACAO_SIMPLIFICADO)
        self.assertEqual(classifier.classificar("1"), "Assets:Ativo")
        self.assertEqual(classifier.classificar("11"), "Assets:Ativo-Circulante")
        self.assertEqual(classifier.classificar("2"), "Liabilities")
        self.assertEqual(classifier.classificar("9"), "Income:Receitas")
        self.assertEqual(classifier.classificar("93"), "Expenses:Custos-dos-Bens-e-Servicos-Vendidos")

    def test_modelo_ifrs(self):
        """Testa uso do modelo IFRS."""
        classificacao = obter_classificacao_do_modelo(TipoPlanoContas.IFRS)
        classifier = AccountClassifier(classificacao)
        
        self.assertEqual(classifier.mapeamento, CLASSIFICACAO_IFRS)
        self.assertEqual(classifier.classificar("1"), "Assets")
        self.assertEqual(classifier.classificar("11"), "Assets:Current")
        self.assertEqual(classifier.classificar("12"), "Assets:Non-Current")
        self.assertEqual(classifier.classificar("2"), "Liabilities")
        self.assertEqual(classifier.classificar("21"), "Liabilities:Current")
        self.assertEqual(classifier.classificar("22"), "Liabilities:Non-Current")

    def test_criar_com_modelo(self):
        """Testa criação de classifier usando obter_classificacao_do_modelo."""
        classificacao = obter_classificacao_do_modelo(TipoPlanoContas.SIMPLIFICADO)
        classifier = AccountClassifier(classificacao)
        
        self.assertEqual(classifier.mapeamento, CLASSIFICACAO_SIMPLIFICADO)
        self.assertEqual(classifier.classificar("1"), "Assets:Ativo")

    def test_obter_modelos_disponiveis(self):
        """Testa método obter_modelos_disponiveis."""
        modelos = AccountClassifier.obter_modelos_disponiveis()
        
        self.assertIsInstance(modelos, list)
        self.assertGreater(len(modelos), 0)
        self.assertIn(TipoPlanoContas.PADRAO, modelos)
        self.assertIn(TipoPlanoContas.SIMPLIFICADO, modelos)
        self.assertIn(TipoPlanoContas.IFRS, modelos)

    def test_prioridade_customizado_sobre_modelo(self):
        """Testa que mapeamento customizado tem prioridade sobre modelo."""
        custom = {"1": "Assets:Custom"}
        # Usa obter_classificacao_do_modelo com customizações
        classificacao = obter_classificacao_do_modelo(TipoPlanoContas.SIMPLIFICADO, custom)
        classifier = AccountClassifier(classificacao)
        
        # Deve usar o customizado, não o modelo
        self.assertEqual(classifier.mapeamento["1"], "Assets:Custom")
        self.assertEqual(classifier.classificar("1"), "Assets:Custom")


if __name__ == "__main__":
    unittest.main()

