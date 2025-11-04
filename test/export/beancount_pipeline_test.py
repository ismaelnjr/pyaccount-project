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

from pyaccount import BeancountPipeline, OpeningBalancesBuilder
from pyaccount.core.account_classifier import TipoPlanoContas
from datetime import date, timedelta
from dateutil.parser import parse as parse_date

class TestBeancountPipeline(unittest.TestCase):

    def test_beancount_pipeline(self):
        """Testa geração de arquivo Beancount para um período específico."""
        
        empresa = 267
        inicio_periodo = date(2025, 1, 1)
        fim_periodo = date(2025, 12, 31)
        dia_anterior = inicio_periodo - timedelta(days=1)  
        
        # Passo 1: Gera saldos de abertura em 
        print(f"\n--- Passo 1: Gerando saldos de abertura em {dia_anterior} ---")
        builder_saldos = OpeningBalancesBuilder(
            dsn="Local_17",
            user="consulta",
            password="consulta",
            empresa=empresa,
            ate=dia_anterior,
            saida="./out",
            modelo=TipoPlanoContas.SIMPLIFICADO,
            
        )
        saldos_abertura_path = builder_saldos.execute()
        print(f"✓ Saldos de abertura gerados: {saldos_abertura_path}")
        
        # Verifica se o arquivo foi criado
        self.assertTrue(saldos_abertura_path.exists(), f"Arquivo {saldos_abertura_path} não foi criado")
        
        # Passo 2: Gera arquivo Beancount usando saldos de abertura como cache
        print(f"\n--- Passo 2: Gerando arquivo Beancount para período {inicio_periodo} a {fim_periodo} ---")
        pipeline = BeancountPipeline(
            dsn="Local_17",
            user="consulta",
            password="consulta",
            empresa=empresa,
            inicio=inicio_periodo,
            fim=fim_periodo,
            moeda="BRL",
            outdir="./out",
            saldos_path=str(saldos_abertura_path),
            modelo=TipoPlanoContas.SIMPLIFICADO,
        )
        bean_path = pipeline.execute()
        print(f"✓ Arquivo Beancount gerado: {bean_path}")
        
        # Verifica se o arquivo Beancount foi criado
        self.assertTrue(bean_path.exists(), f"Arquivo {bean_path} não foi criado")
        
        # Verifica se o arquivo tem conteúdo
        self.assertGreater(bean_path.stat().st_size, 0, "Arquivo Beancount está vazio")
        
        # Passo 3: Validação dos arquivos gerados
        print("\n--- Passo 3: Validando arquivos gerados ---")
        
        # Verifica arquivo Beancount principal
        print(f"✓ Validando arquivo Beancount: {bean_path.name}")
        with bean_path.open("r", encoding="utf-8") as f:
            bean_content = f.read()
            # Verifica cabeçalho básico
            self.assertIn("option \"operating_currency\"", bean_content, "Arquivo deve conter configuração de moeda")
            self.assertIn("BRL", bean_content, "Arquivo deve conter moeda BRL")
            self.assertIn("open", bean_content, "Arquivo deve conter declarações open")
            self.assertIn("Abertura de saldos", bean_content, "Arquivo deve conter transação de abertura")
            print(f"  - Arquivo contém {len(bean_content)} caracteres")
            print(f"  - Contém declarações open e transações")
        
        # Verifica mapa de contas CSV
        mapa_path = Path("./out") / f"mapa_beancount_{empresa}.csv"
        self.assertTrue(mapa_path.exists(), f"Arquivo mapa {mapa_path} não foi criado")
        print(f"✓ Mapa de contas gerado: {mapa_path.name}")
        df_mapa = pd.read_csv(mapa_path, sep=";", encoding="utf-8-sig")
        self.assertGreater(len(df_mapa), 0, "Mapa de contas está vazio")
        colunas_mapa_esperadas = ["CLAS_CTA", "NOME_CTA", "BC_ACCOUNT"]
        for col in colunas_mapa_esperadas:
            self.assertIn(col, df_mapa.columns, f"Coluna {col} não encontrada no mapa")
        print(f"  - Mapa contém {len(df_mapa)} contas mapeadas")
        
        # Verifica balancete de abertura CSV
        bal_abertura_path = Path("./out") / f"balancete_abertura_{empresa}_{dia_anterior}.csv"
        self.assertTrue(bal_abertura_path.exists(), f"Arquivo balancete {bal_abertura_path} não foi criado")
        print(f"✓ Balancete de abertura gerado: {bal_abertura_path.name}")
        df_balancete = pd.read_csv(bal_abertura_path, sep=";", encoding="utf-8-sig")
        self.assertGreater(len(df_balancete), 0, "Balancete de abertura está vazio")
        colunas_balancete_esperadas = ["BC_ACCOUNT", "saldo"]
        for col in colunas_balancete_esperadas:
            self.assertIn(col, df_balancete.columns, f"Coluna {col} não encontrada no balancete")
        print(f"  - Balancete contém {len(df_balancete)} contas com saldo")
        
        # Passo 4: Validação adicional do conteúdo Beancount
        print("\n--- Passo 4: Validação adicional do arquivo Beancount ---")
        
        # Conta linhas de transações
        inicio_str = inicio_periodo.strftime("%Y-%m")
        linhas_transacoes = [l for l in bean_content.split("\n") if l.strip().startswith(inicio_str)]
        print(f"  - Encontradas {len(linhas_transacoes)} linhas de transações no período")
        
        # Verifica que há declarações open
        linhas_open = [l for l in bean_content.split("\n") if " open " in l]
        self.assertGreater(len(linhas_open), 0, "Arquivo deve conter declarações open")
        print(f"  - Encontradas {len(linhas_open)} declarações open")
        
        # Verifica transação de abertura
        self.assertIn(f"{inicio_periodo} * \"Abertura de saldos\"", bean_content, 
                     "Arquivo deve conter transação de abertura no início do período")
        print(f"  - Transação de abertura encontrada")
        
        # Verifica que há lançamentos do período
        inicio_str_full = inicio_periodo.strftime("%Y-%m-%d")
        fim_str_full = fim_periodo.strftime("%Y-%m-%d")
        def data_no_periodo(linha):
            """Verifica se a linha começa com uma data no período."""
            if len(linha) < 10:
                return False
            try:
                data_linha = parse_date(linha[:10]).date()
                return inicio_periodo <= data_linha <= fim_periodo
            except:
                return False
        
        linhas_lancamentos = [l for l in bean_content.split("\n") 
                              if l.strip() and not l.strip().startswith(";") 
                              and not l.strip().startswith("option")
                              and "open" not in l
                              and "Abertura de saldos" not in l
                              and (data_no_periodo(l) or l.strip().startswith("  "))]
        # Filtra apenas linhas que são transações (não comentários ou opções)
        transacoes = [l for l in bean_content.split("\n") 
                     if l.strip() and l.strip()[0].isdigit() and " * " in l]
        print(f"  - Encontradas {len(transacoes)} transações (incluindo abertura)")
        
        # Passo 5: Validação específica das movimentações do período com histórico por data
        print("\n--- Passo 5: Validação das movimentações do período ---")
        
        # Separa transações de abertura das movimentações do período
        transacoes_abertura = [l for l in bean_content.split("\n") 
                             if f"{inicio_periodo} * \"Abertura de saldos\"" in l]
        transacoes_periodo = [l for l in bean_content.split("\n") 
                             if l.strip() and l.strip()[0].isdigit() 
                             and " * " in l 
                             and "Abertura de saldos" not in l]
        
        # Verifica se há movimentações além da abertura
        if len(transacoes_periodo) == 0:
            print(f"  - Aviso: Nenhuma transação do período encontrada (apenas abertura)")
            print(f"  - Isso pode ocorrer se não houver lançamentos no período ou se todos foram filtrados")
        else:
            print(f"  - Encontradas {len(transacoes_periodo)} transações do período (excluindo abertura)")
            
            # Valida formato de cada transação do período
            transacoes_validadas = 0
            for transacao in transacoes_periodo[:20]:  # Valida as 20 primeiras como exemplo
                if not transacao.strip():
                    continue
                parts = transacao.split(" * ")
                self.assertEqual(len(parts), 2, f"Transação mal formatada: {transacao}")
                
                # Valida data
                data_part = parts[0].strip()
                self.assertRegex(data_part, r"^\d{4}-\d{2}-\d{2}$", 
                                f"Data mal formatada em: {transacao}")
                
                # Valida que data está no período
                try:
                    data_transacao = parse_date(data_part).date()
                    self.assertGreaterEqual(data_transacao, inicio_periodo,
                                           f"Data {data_transacao} está antes do início do período")
                    self.assertLessEqual(data_transacao, fim_periodo,
                                        f"Data {data_transacao} está depois do fim do período")
                except Exception as e:
                    self.fail(f"Erro ao parsear data em {transacao}: {e}")
                
                # Valida histórico (pode ter meta após as aspas)
                hist_part = parts[1].strip()
                # O histórico pode ser "histórico" "meta" ou apenas "histórico"
                # Verifica que começa com aspas
                self.assertTrue(hist_part.startswith('"'),
                               f"Histórico deve começar com aspas em: {transacao}")
                # Pega a primeira parte entre aspas
                if '"' in hist_part[1:]:
                    # Há meta adicional: "histórico" "meta"
                    hist_sem_meta = hist_part.split('"')[1]
                else:
                    # Apenas histórico: "histórico"
                    hist_sem_meta = hist_part.strip('"')
                self.assertGreater(len(hist_sem_meta), 0,
                                 f"Histórico não pode estar vazio em: {transacao}")
                
                transacoes_validadas += 1
            
            print(f"  - Formato de {transacoes_validadas} transações validado (data, histórico)")
        
        # Valida que cada transação tem débito e crédito formatados
        linhas_beans = bean_content.split("\n")
        transacoes_com_valores = 0
        for i, linha in enumerate(linhas_beans):
            if linha.strip() and linha.strip()[0].isdigit() and " * " in linha:
                # Próxima linha deve ser débito
                if i + 1 < len(linhas_beans):
                    linha_deb = linhas_beans[i + 1]  # Não fazer strip ainda
                    if linha_deb.startswith("  ") and "BRL" in linha_deb:
                        # Segunda linha seguinte deve ser crédito
                        if i + 2 < len(linhas_beans):
                            linha_cre = linhas_beans[i + 2]  # Não fazer strip ainda
                            if linha_cre.startswith("  ") and "BRL" in linha_cre:
                                transacoes_com_valores += 1
        
        if transacoes_com_valores > 0:
            print(f"  - {transacoes_com_valores} transações com débito e crédito formatados encontradas")
        else:
            print(f"  - Aviso: Nenhuma transação encontrada com débito e crédito formatados (pode não haver lançamentos no período)")
        
        # Conta movimentações por data
        movimentacoes_por_data = {}
        for transacao in transacoes_periodo:
            if not transacao.strip():
                continue
            parts = transacao.split(" * ")
            if len(parts) >= 1:
                data_part = parts[0].strip()
                if data_part in movimentacoes_por_data:
                    movimentacoes_por_data[data_part] += 1
                else:
                    movimentacoes_por_data[data_part] = 1
        
        if len(movimentacoes_por_data) > 0:
            print(f"  - Movimentações encontradas em {len(movimentacoes_por_data)} datas diferentes")
            exemplo_data = list(movimentacoes_por_data.keys())[0]
            exemplo_qtde = movimentacoes_por_data[exemplo_data]
            print(f"  - Exemplo: {exemplo_data} - {exemplo_qtde} transação(ões)")
        else:
            print(f"  - Aviso: Nenhuma movimentação encontrada por data (apenas abertura)")
        
        print(f"\n✓ Teste concluído com sucesso!")
        print(f"  - Arquivo Beancount: {bean_path}")
        print(f"  - Mapa de contas: {mapa_path}")
        print(f"  - Balancete de abertura: {bal_abertura_path}")
        print(f"  - Saldos de abertura usados: {saldos_abertura_path}")

if __name__ == '__main__':
    unittest.main()

