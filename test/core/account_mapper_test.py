import os
import sys
import unittest
import pandas as pd

# Necessário para que o arquivo de testes encontre
test_file_dir = os.path.dirname(os.path.abspath(__file__))
test_dir = os.path.dirname(test_file_dir)  # test/
project_root = os.path.dirname(test_dir)  # raiz do projeto
os.chdir(test_dir)  # muda para test/ para que caminhos relativos funcionem
sys.path.insert(0, project_root)

from pyaccount.core.account_mapper import AccountMapper
from pyaccount.core.utils import normalizar_nome


class TestAccountMapper(unittest.TestCase):
    """Testes para a classe AccountMapper."""

    def test_init_com_classificacao_padrao(self):
        """Testa inicialização com classificação padrão."""
        mapper = AccountMapper()
        
        self.assertIsNotNone(mapper.classifier)
        self.assertIsNone(mapper.custom_classifier)

    def test_init_com_classificacao_customizada(self):
        """Testa inicialização com classificação customizada."""
        classificacao_custom = {
            "1": "Assets:Customizado",
            "2": "Liabilities:Custom"
        }
        mapper = AccountMapper(classificacao_custom)
        
        self.assertIsNotNone(mapper.classifier)
        self.assertEqual(mapper.custom_classifier, classificacao_custom)

    def test_classificar_beancount(self):
        """Testa método classificar_beancount."""
        mapper = AccountMapper()
        
        # Delega para o classifier
        resultado = mapper.classificar_beancount("11")
        self.assertEqual(resultado, "Assets:Ativo-Circulante")
        
        resultado = mapper.classificar_beancount("31")
        self.assertEqual(resultado, "Expenses:Custos")
        
        # Com classificação customizada
        classificacao_custom = {"1": "Assets:Custom"}
        mapper = AccountMapper(classificacao_custom)
        resultado = mapper.classificar_beancount("1")
        self.assertEqual(resultado, "Assets:Custom")

    def test_criar_bc_account_com_hierarquia(self):
        """Testa criação de conta Beancount com grupo hierárquico."""
        mapper = AccountMapper()
        
        # BC_GROUP já contém ":"
        resultado = mapper.criar_bc_account("Assets:Ativo-Circulante", "Caixa")
        self.assertEqual(resultado, "Assets:Ativo-Circulante:Caixa")
        
        resultado = mapper.criar_bc_account("Liabilities:Passivo-Circulante", "Fornecedores")
        self.assertEqual(resultado, "Liabilities:Passivo-Circulante:Fornecedores")

    def test_criar_bc_account_sem_hierarquia(self):
        """Testa criação de conta Beancount com grupo não hierárquico."""
        mapper = AccountMapper()
        
        # BC_GROUP não contém ":" (será normalizado)
        resultado = mapper.criar_bc_account("Assets", "Caixa")
        self.assertEqual(resultado, "Assets:Caixa")
        
        # Testa com espaços e caracteres especiais (serão normalizados)
        resultado = mapper.criar_bc_account("Ativo Circulante", "Caixa")
        self.assertEqual(resultado, "Ativo-Circulante:Caixa")

    def test_processar_plano_contas(self):
        """Testa processamento de plano de contas."""
        mapper = AccountMapper()
        
        df_pc = pd.DataFrame({
            "CODI_CTA": ["101", "201", "301"],
            "CLAS_CTA": ["11", "21", "31"],
            "TIPO_CTA": ["A", "S", "A"],
            "NOME_CTA": ["Caixa", "Fornecedores", "Custos de Vendas"]
        })
        
        df_processado = mapper.processar_plano_contas(df_pc)
        
        # Verifica colunas adicionadas
        self.assertIn("BC_GROUP", df_processado.columns)
        self.assertIn("BC_NAME", df_processado.columns)
        self.assertIn("BC_ACCOUNT", df_processado.columns)
        
        # Verifica valores
        self.assertEqual(df_processado.iloc[0]["BC_GROUP"], "Assets:Ativo-Circulante")
        self.assertEqual(df_processado.iloc[0]["BC_NAME"], "Caixa")
        self.assertEqual(df_processado.iloc[0]["BC_ACCOUNT"], "Assets:Ativo-Circulante:Caixa")
        
        self.assertEqual(df_processado.iloc[1]["BC_GROUP"], "Liabilities:Passivo-Circulante")
        self.assertEqual(df_processado.iloc[1]["BC_NAME"], "Fornecedores")
        
        self.assertEqual(df_processado.iloc[2]["BC_GROUP"], "Expenses:Custos")
        self.assertEqual(df_processado.iloc[2]["BC_NAME"], "Custos-De-Vendas")

    def test_processar_plano_contas_filtrar_ativas(self):
        """Testa processamento de plano de contas filtrando apenas contas ativas."""
        mapper = AccountMapper()
        
        df_pc = pd.DataFrame({
            "CODI_CTA": ["101", "201", "301"],
            "CLAS_CTA": ["11", "21", "31"],
            "TIPO_CTA": ["A", "S", "A"],
            "NOME_CTA": ["Caixa", "Fornecedores", "Custos"],
            "SITUACAO_CTA": ["A", "I", "A"]  # I = Inativa
        })
        
        df_processado = mapper.processar_plano_contas(df_pc, filtrar_ativas=True)
        
        # Deve ter apenas 2 contas (as ativas)
        self.assertEqual(len(df_processado), 2)
        self.assertEqual(df_processado.iloc[0]["CODI_CTA"], "101")
        self.assertEqual(df_processado.iloc[1]["CODI_CTA"], "301")

    def test_processar_plano_contas_vazio(self):
        """Testa processamento de plano de contas vazio."""
        mapper = AccountMapper()
        
        df_pc = pd.DataFrame()
        
        with self.assertRaises(ValueError) as context:
            mapper.processar_plano_contas(df_pc)
        
        self.assertIn("vazio", str(context.exception).lower())

    def test_processar_plano_contas_com_unknown(self):
        """Testa processamento de plano de contas com CLAS_CTA não mapeada."""
        mapper = AccountMapper()
        
        df_pc = pd.DataFrame({
            "CODI_CTA": ["888"],
            "CLAS_CTA": ["88"],  # Não mapeado (não usa "9" pois está mapeado)
            "TIPO_CTA": ["A"],
            "NOME_CTA": ["Conta Desconhecida"]
        })
        
        df_processado = mapper.processar_plano_contas(df_pc)
        
        self.assertEqual(df_processado.iloc[0]["BC_GROUP"], "Unknown")
        self.assertEqual(df_processado.iloc[0]["BC_ACCOUNT"], "Unknown:Conta-Desconhecida")

    def test_criar_mapas(self):
        """Testa criação de mapas de lookup."""
        mapper = AccountMapper()
        
        df_pc = pd.DataFrame({
            "CODI_CTA": ["101", "201", "301"],
            "CLAS_CTA": ["11", "21", "31"],
            "TIPO_CTA": ["A", "S", "A"],
            "NOME_CTA": ["Caixa", "Fornecedores", "Custos"],
            "BC_ACCOUNT": [
                "Assets:Ativo-Circulante:Caixa",
                "Liabilities:Passivo-Circulante:Fornecedores",
                "Expenses:Custos:Custos"
            ]
        })
        
        mapas = mapper.criar_mapas(df_pc)
        
        # Verifica estrutura dos mapas
        self.assertIn("clas_to_bc", mapas)
        self.assertIn("codi_to_bc", mapas)
        
        # Verifica mapeamento por classificação
        self.assertEqual(mapas["clas_to_bc"]["11"], "Assets:Ativo-Circulante:Caixa")
        self.assertEqual(mapas["clas_to_bc"]["21"], "Liabilities:Passivo-Circulante:Fornecedores")
        self.assertEqual(mapas["clas_to_bc"]["31"], "Expenses:Custos:Custos")
        
        # Verifica mapeamento por código
        self.assertEqual(mapas["codi_to_bc"]["101"], "Assets:Ativo-Circulante:Caixa")
        self.assertEqual(mapas["codi_to_bc"]["201"], "Liabilities:Passivo-Circulante:Fornecedores")
        self.assertEqual(mapas["codi_to_bc"]["301"], "Expenses:Custos:Custos")

    def test_criar_mapas_com_duplicatas(self):
        """Testa criação de mapas com contas duplicadas (última prevalece)."""
        mapper = AccountMapper()
        
        df_pc = pd.DataFrame({
            "CODI_CTA": ["101", "101"],  # Duplicado
            "CLAS_CTA": ["11", "11"],
            "TIPO_CTA": ["A", "A"],
            "NOME_CTA": ["Caixa", "Caixa-Outro"],
            "BC_ACCOUNT": [
                "Assets:Ativo-Circulante:Caixa",
                "Assets:Ativo-Circulante:Caixa-Outro"
            ]
        })
        
        mapas = mapper.criar_mapas(df_pc)
        
        # Última conta deve prevalecer
        self.assertEqual(mapas["codi_to_bc"]["101"], "Assets:Ativo-Circulante:Caixa-Outro")

    def test_integracao_completa(self):
        """Testa integração completa: processar plano de contas e criar mapas."""
        mapper = AccountMapper()
        
        df_pc = pd.DataFrame({
            "CODI_CTA": ["101", "201"],
            "CLAS_CTA": ["11", "21"],
            "TIPO_CTA": ["A", "S"],
            "NOME_CTA": ["Caixa", "Fornecedores"]
        })
        
        # Processa plano de contas
        df_processado = mapper.processar_plano_contas(df_pc)
        
        # Cria mapas
        mapas = mapper.criar_mapas(df_processado)
        
        # Verifica integração
        self.assertIn("101", mapas["codi_to_bc"])
        self.assertIn("11", mapas["clas_to_bc"])
        
        # Verifica que BC_ACCOUNT nos mapas corresponde ao processado
        conta_101 = df_processado[df_processado["CODI_CTA"] == "101"].iloc[0]
        self.assertEqual(mapas["codi_to_bc"]["101"], conta_101["BC_ACCOUNT"])

    def test_normalizar_nome_com_padrao_contra_ativo_com_espacos(self):
        """Testa normalização de nome com padrão '( - )' e variações."""
        # Testa o caso específico reportado: "( - ) DEPRECIAÇÃO ACUMULADA MOVEIS E UTENS"
        resultado = normalizar_nome("( - ) DEPRECIAÇÃO ACUMULADA MOVEIS E UTENS")
        self.assertEqual(resultado, "Depreciacao-Acumulada-Moveis-E-Utens")
        # Verifica que não começa com hífen
        self.assertFalse(resultado.startswith("-"))
        # Verifica que não tem hífens duplicados
        self.assertNotIn("--", resultado)
        
        # Testa outras variações do padrão
        resultado2 = normalizar_nome("( -) DEPRECIAÇÃO ACUMULADA")
        self.assertEqual(resultado2, "Depreciacao-Acumulada")
        self.assertFalse(resultado2.startswith("-"))
        
        resultado3 = normalizar_nome("(- ) DEPRECIAÇÃO ACUMULADA")
        self.assertEqual(resultado3, "Depreciacao-Acumulada")
        self.assertFalse(resultado3.startswith("-"))
        
        # Testa o padrão original "(-)" que deve ser tratado como antes
        resultado4 = normalizar_nome("(-) DEPRECIAÇÃO ACUMULADA")
        self.assertEqual(resultado4, "Depreciacao-Acumulada")
        self.assertFalse(resultado4.startswith("-"))

    def test_normalizar_nome_sem_hifens_duplicados(self):
        """Testa que nomes normalizados não têm hífens duplicados."""
        # Testa com múltiplos hífens consecutivos
        resultado = normalizar_nome("CONTA -- COM -- HIFENS")
        self.assertNotIn("--", resultado)
        self.assertEqual(resultado, "Conta-Com-Hifens")
        
        # Testa com hífens no meio
        resultado2 = normalizar_nome("CONTA---COM---MUITOS---HIFENS")
        self.assertNotIn("--", resultado2)
        self.assertEqual(resultado2, "Conta-Com-Muitos-Hifens")

    def test_normalizar_nome_sem_hifens_inicio_fim(self):
        """Testa que nomes normalizados não começam nem terminam com hífen."""
        # Testa com hífen no início
        resultado = normalizar_nome("-CONTA INICIO")
        self.assertFalse(resultado.startswith("-"))
        self.assertEqual(resultado, "Conta-Inicio")
        
        # Testa com hífen no fim
        resultado2 = normalizar_nome("CONTA FIM-")
        self.assertFalse(resultado2.endswith("-"))
        self.assertEqual(resultado2, "Conta-Fim")
        
        # Testa com hífens em ambos os lados
        resultado3 = normalizar_nome("-CONTA AMBOS-")
        self.assertFalse(resultado3.startswith("-"))
        self.assertFalse(resultado3.endswith("-"))
        self.assertEqual(resultado3, "Conta-Ambos")

    def test_normalizar_nome_integracao_com_account_mapper(self):
        """Testa integração da normalização com AccountMapper."""
        mapper = AccountMapper()
        
        # Testa com o caso específico reportado
        df_pc = pd.DataFrame({
            "CODI_CTA": ["101"],
            "CLAS_CTA": ["11"],
            "TIPO_CTA": ["A"],
            "NOME_CTA": ["( - ) DEPRECIAÇÃO ACUMULADA MOVEIS E UTENS"]
        })
        
        df_processado = mapper.processar_plano_contas(df_pc)
        
        # Verifica que BC_NAME não tem hífens duplicados ou no início
        bc_name = df_processado.iloc[0]["BC_NAME"]
        self.assertNotIn("--", bc_name)
        self.assertFalse(bc_name.startswith("-"))
        self.assertEqual(bc_name, "Depreciacao-Acumulada-Moveis-E-Utens")
        
        # Verifica que BC_ACCOUNT também está correto
        bc_account = df_processado.iloc[0]["BC_ACCOUNT"]
        self.assertNotIn("--", bc_account)
        self.assertFalse(bc_account.startswith(":"))
        # Verifica que não há hífens duplicados após os dois pontos
        partes = bc_account.split(":")
        for parte in partes:
            self.assertNotIn("--", parte)
            if parte:  # Se não for vazia
                self.assertFalse(parte.startswith("-"))
                self.assertFalse(parte.endswith("-"))


if __name__ == "__main__":
    unittest.main()

