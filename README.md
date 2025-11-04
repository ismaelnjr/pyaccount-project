# PyAccount - Ferramentas para Importação de Dados Contábeis

Sistema completo para importação de dados contábeis de sistemas ERP (Betha/SQL Anywhere via ODBC) para Beancount e exportação para Excel.

## Características

- ✅ **Extração de dados** via ODBC (SQL Anywhere/Betha)
- ✅ **Geração de arquivos Beancount** com saldos iniciais e transações
- ✅ **Exportação para Excel** com múltiplas abas (Plano de Contas, Balanço Patrimonial, DRE, Balancete, Movimentação)
- ✅ **Múltiplos modelos de classificação** (Padrão BR, Simplificado, IFRS)
- ✅ **Classificação customizável** por empresa
- ✅ **Estrutura modular** e extensível

## Estrutura do Projeto

```
pyaccount/
├── core/                    # Funções base compartilhadas
│   ├── account_classifier.py    # Classificação de contas (múltiplos modelos)
│   ├── account_mapper.py        # Mapeamento de contas para Beancount
│   └── utils.py                 # Utilitários (normalização, formatação)
│
├── data/                    # Acesso a dados
│   └── db_client.py             # Cliente ODBC para SQL Anywhere
│
├── builders/                 # Construtores de relatórios
│   ├── financial_statements.py  # Balanço, DRE, Balancete, Movimentação
│   └── opening_balances.py      # Saldos iniciais/abertura
│
└── export/                   # Exportadores
    ├── beancount_pipeline.py    # Pipeline principal Beancount
    └── exporters.py             # Exportadores (Beancount, Excel)

test/
├── core/                    # Testes das funções base
├── data/                    # Testes de acesso a dados
├── builders/                # Testes dos construtores
└── export/                  # Testes dos exportadores
```

## Instalação

```bash
python -m venv .venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
```

> **ODBC**: Configure um DSN para SQL Anywhere (ex.: `SQLANYWHERE17`).

## Uso Rápido

### 1. Geração de arquivo Beancount

```bash
# Via argumentos
python -m pyaccount.export.beancount_pipeline \
    --dsn SQLANYWHERE17 --user dba --password sql \
    --empresa 437 --inicio 2025-09-01 --fim 2025-09-18 \
    --saida ./out --moeda BRL

# Com arquivo de configuração
python -m pyaccount.export.beancount_pipeline \
    --config ./config.ini \
    --empresa 437 --inicio 2025-09-01 --fim 2025-09-18 \
    --saida ./out
```

### 2. Geração de saldos iniciais (cache)

```bash
python -m pyaccount.builders.opening_balances \
    --dsn SQLANYWHERE17 --user dba --password sql \
    --empresa 437 --ate 2025-08-31 \
    --saida ./out
```

### 3. Exportação para Excel

Atualmente, a exportação para Excel deve ser feita via código Python (não há CLI separada). Veja seção "Uso Programático" abaixo.

## Uso Programático

### AccountClassifier vs AccountMapper

O sistema possui duas classes principais para trabalhar com contas contábeis:

#### AccountClassifier (Classificador)
**Responsabilidade:** Classificar contas contábeis em categorias Beancount baseado apenas em `CLAS_CTA`.

- **Entrada:** `CLAS_CTA` (ex: "11", "31", "4")
- **Saída:** Categoria Beancount (ex: "Assets:Ativo-Circulante", "Expenses:Custos")
- **Foco:** Apenas classificação (CLAS_CTA → categoria)
- **Uso:** Quando você precisa apenas classificar uma conta específica

```python
from pyaccount.core.account_classifier import AccountClassifier, obter_classificacao_do_modelo, TipoPlanoContas

# Obter classificação do modelo
classificacao = obter_classificacao_do_modelo(TipoPlanoContas.PADRAO)
classifier = AccountClassifier(classificacao)

# Classificar uma conta
grupo = classifier.classificar("11")  # Retorna: "Assets:Ativo-Circulante"
grupo = classifier.classificar("31")  # Retorna: "Expenses:Custos"
```

#### AccountMapper (Mapeador)
**Responsabilidade:** Mapear contas contábeis completas para Beancount, incluindo classificação, normalização de nomes e criação de contas hierárquicas.

- **Entrada:** DataFrame com plano de contas completo (CLAS_CTA, NOME_CTA, CODI_CTA, etc.)
- **Saída:** DataFrame processado com colunas BC_GROUP, BC_NAME, BC_ACCOUNT
- **Foco:** Processamento completo (classificação + normalização + estrutura hierárquica)
- **Uso:** Quando você precisa processar um plano de contas completo

```python
from pyaccount.core.account_mapper import AccountMapper
from pyaccount.core.account_classifier import obter_classificacao_do_modelo, TipoPlanoContas

# Obter classificação do modelo
classificacao = obter_classificacao_do_modelo(TipoPlanoContas.PADRAO)
mapper = AccountMapper(classificacao)

# Processar plano de contas completo
df_processado = mapper.processar_plano_contas(df_pc)
# Resultado: DataFrame com BC_GROUP, BC_NAME, BC_ACCOUNT
# Exemplo: "Assets:Ativo-Circulante:Caixa"

# Criar mapas de lookup
mapas = mapper.criar_mapas(df_processado)
conta_beancount = mapas["codi_to_bc"]["101"]  # Ex: "Assets:Ativo-Circulante:Caixa"
```

#### Relação entre eles

```
AccountMapper
    └── usa AccountClassifier internamente (via self.classifier)
    └── adiciona normalização de nomes (via normalizar_nome)
    └── adiciona criação de contas hierárquicas (via criar_bc_account)
    └── adiciona processamento de DataFrames completos
```

**Resumo:**
- **AccountClassifier**: Classificação básica (CLAS_CTA → categoria)
- **AccountMapper**: Processamento completo (usa AccountClassifier + normalização + hierarquia)

### Classificação de Contas

O sistema suporta múltiplos modelos de classificação. Para usar modelos, primeiro obtenha o dicionário de classificação:

```python
from pyaccount.core.account_classifier import (
    AccountClassifier,
    obter_classificacao_do_modelo,
    TipoPlanoContas
)

# Obter classificação do modelo padrão
classificacao = obter_classificacao_do_modelo(TipoPlanoContas.PADRAO)
classifier = AccountClassifier(classificacao)

# Modelo específico
classificacao_ifrs = obter_classificacao_do_modelo(TipoPlanoContas.IFRS)
classifier_ifrs = AccountClassifier(classificacao_ifrs)

classificacao_simplificado = obter_classificacao_do_modelo(TipoPlanoContas.SIMPLIFICADO)
classifier_simplificado = AccountClassifier(classificacao_simplificado)

# Classificação customizada (tem prioridade sobre modelo)
classificacao_custom = obter_classificacao_do_modelo(
    TipoPlanoContas.PADRAO,
    customizacoes={
        "1": "Assets:Customizado",
        "2": "Liabilities:Custom"
    }
)
classifier_custom = AccountClassifier(classificacao_custom)

# Classificar uma conta
grupo = classifier.classificar("11210100708")  # Retorna "Assets:Ativo-Circulante"

# Listar modelos disponíveis
from pyaccount.core.account_classifier import MODELOS_CLASSIFICACAO
modelos = list(MODELOS_CLASSIFICACAO.keys())
for modelo in modelos:
    print(f"{modelo.value}: {modelo}")
```

### Mapeamento de Contas

```python
from pyaccount.core.account_mapper import AccountMapper
from pyaccount.core.account_classifier import obter_classificacao_do_modelo, TipoPlanoContas

# Obter classificação do modelo e criar mapeador
classificacao = obter_classificacao_do_modelo(TipoPlanoContas.PADRAO)
mapper = AccountMapper(classificacao)

# Ou com classificação customizada
classificacao_custom = obter_classificacao_do_modelo(
    TipoPlanoContas.PADRAO,
    customizacoes={
        "1": "Assets:Customizado",
        "2": "Liabilities:Custom"
    }
)
mapper = AccountMapper(classificacao_custom)

# Processar plano de contas
df_processado = mapper.processar_plano_contas(df_pc)

# Criar mapas de lookup
mapas = mapper.criar_mapas(df_processado)
conta_beancount = mapas["codi_to_bc"]["101"]  # Ex: "Assets:Ativo-Circulante:Caixa"
```

### Pipeline Beancount

```python
from pyaccount import BeancountPipeline
from datetime import date

pipeline = BeancountPipeline(
    dsn="SQLANYWHERE17",
    user="dba",
    password="sql",
    empresa=437,
    inicio=date(2025, 9, 1),
    fim=date(2025, 9, 18),
    moeda="BRL",
    outdir="./out",
    classificacao_customizada=None  # Ou dicionário customizado
)

bean_path = pipeline.execute()
```

### Exportação para Excel

```python
from pyaccount import ExcelExporter, ContabilDBClient
from datetime import date
from pathlib import Path

db_client = ContabilDBClient(dsn="SQLANYWHERE17", user="dba", password="sql")
exporter = ExcelExporter(
    db_client=db_client,
    empresa=437,
    inicio=date(2025, 9, 1),
    fim=date(2025, 9, 18),
    classificacao_customizada=None  # Ou dicionário customizado
)

# exportar_excel aceita outdir (Path) e opcionalmente nome_arquivo
excel_path = exporter.exportar_excel(Path("./out"))
# Ou especificar nome do arquivo
excel_path = exporter.exportar_excel(Path("./out"), "contabilidade.xlsx")
print(f"Arquivo Excel gerado: {excel_path}")
```

### Construtores de Relatórios

```python
from pyaccount import (
    BalanceSheetBuilder,
    IncomeStatementBuilder,
    TrialBalanceBuilder,
    PeriodMovementsBuilder,
    AccountMapper
)

mapper = AccountMapper()

# Balanço Patrimonial
balanco_builder = BalanceSheetBuilder(df_saldos_finais, df_plano_contas, mapper)
df_balanco = balanco_builder.gerar()

# DRE
dre_builder = IncomeStatementBuilder(df_movimentacoes, df_plano_contas, mapper)
df_dre = dre_builder.gerar()

# Balancete
balancete_builder = TrialBalanceBuilder(
    df_plano_contas, df_saldos_iniciais, df_lancamentos, mapper
)
df_balancete = balancete_builder.gerar()
```

## Modelos de Classificação

O sistema oferece três modelos pré-configurados:

### 1. PADRAO (CLASSIFICACAO_PADRAO_BR)
Modelo padrão brasileiro com estrutura completa:
- Ativo (1, 11, 12)
- Passivo e Patrimônio Líquido (2, 21, 22, 23)
- Custos e Despesas (3, 31, 32, 33, 34)
- Receitas (4, 41, 42, 43)
- Contas Transitórias (5)
- Contas de Compensação (9)

### 2. SIMPLIFICADO
Modelo simplificado com estrutura básica:
- Ativo (1, 11, 12)
- Passivo (2, 21, 22, 23)
- Receitas (9, 91, 92)
- Despesas (93, 94, 95, 96, 97, 98, 99)

### 3. IFRS
Modelo baseado em IFRS:
- Assets (Current, Non-Current)
- Liabilities (Current, Non-Current)
- Equity
- Income
- Expenses

## Arquivos de Saída

### Beancount Pipeline
- `lancamentos_<empresa>_<inicio>_<fim>.beancount` - Arquivo Beancount principal
- `mapa_beancount_<empresa>.csv` - Mapa CLAS_CTA → Conta Beancount
- `balancete_abertura_<empresa>_<D-1>.csv` - Balancete de abertura

### Excel Exporter
- `contabilidade_<empresa>_<inicio>_<fim>.xlsx` com abas:
  - **Plano de Contas** - Plano completo com classificação Beancount
  - **Balanço Patrimonial** - Balanço estruturado
  - **DRE** - Demonstração do Resultado do Exercício
  - **Balancete** - Saldo inicial, débitos, créditos e saldo final
  - **Movimentação do Período** - Todas as transações do período

## Configuração Customizada

### Arquivo config.ini

```ini
[database]
dsn = SQLANYWHERE17
user = dba
password = sql

[defaults]
empresa = 437
moeda = BRL

[classification]
clas_1 = Assets:Customizado
clas_11 = Assets:Ativo-Circulante-Custom
clas_2 = Liabilities:Custom
```

### Classificação via código

```python
from pyaccount.core.account_mapper import AccountMapper
from pyaccount.core.account_classifier import obter_classificacao_do_modelo, TipoPlanoContas

# Opção 1: Usar modelo com customizações
classificacao = obter_classificacao_do_modelo(
    TipoPlanoContas.PADRAO,
    customizacoes={
        "1": "Assets:Customizado",
        "11": "Assets:Ativo-Circulante-Custom",
        "2": "Liabilities:Custom"
    }
)
mapper = AccountMapper(classificacao)

# Opção 2: Usar apenas customizações (sem modelo base)
classificacao_custom = {
    "1": "Assets:Customizado",
    "11": "Assets:Ativo-Circulante-Custom",
    "2": "Liabilities:Custom"
}
mapper = AccountMapper(classificacao_custom)
```

## Validações e Integridade

- ✅ **Soma dos saldos de abertura ≈ 0** (débitos - créditos)
- ✅ **Validação de transações balanceadas** por lote
- ✅ **Detecção de contas não mapeadas** com alertas detalhados
- ✅ **Filtro de zeramentos** (opcional, padrão: excluídos)
- ✅ **Classificação de contas desconhecidas** como "Unknown" com alertas

## Testes

```bash
# Executar todos os testes
python -m unittest discover test

# Testes específicos
python -m unittest test.core.account_classifier_test
python -m unittest test.core.account_mapper_test
python -m unittest test.export.beancount_pipeline_test
python -m unittest test.export.excel_exporter_test
```

## Classes Principais

### Core
- **`AccountClassifier`** - Classificação de contas em categorias Beancount baseado em CLAS_CTA
- **`AccountMapper`** - Mapeamento completo de planos de contas (usa AccountClassifier internamente)
- **`obter_classificacao_do_modelo()`** - Função helper para obter dicionário de classificação baseado no modelo
- **`TipoPlanoContas`** - Enum para modelos de classificação (PADRAO, SIMPLIFICADO, IFRS)

### Data
- **`ContabilDBClient`** - Cliente ODBC para acesso ao banco de dados

### Builders
- **`OpeningBalancesBuilder`** - Construção de saldos iniciais
- **`BalanceSheetBuilder`** - Construção do Balanço Patrimonial
- **`IncomeStatementBuilder`** - Construção da DRE
- **`TrialBalanceBuilder`** - Construção do Balancete
- **`PeriodMovementsBuilder`** - Preparação de movimentações

### Export
- **`BeancountPipeline`** - Pipeline completo para geração Beancount
- **`BeancountExporter`** - Exportação para formato Beancount
- **`ExcelExporter`** - Exportação para Excel com múltiplas abas

## Dicas

1. **Cache de saldos**: Use `opening_balances` para gerar cache de saldos e acelerar processamentos
2. **Classificação customizada**: Ajuste os modelos conforme seu plano de contas específico
3. **Filtros de zeramento**: Por padrão, lançamentos de zeramento são excluídos (`desconsiderar_zeramento=True`)
4. **Modelos IFRS**: Adapte `CLASSIFICACAO_IFRS` conforme suas necessidades específicas
5. **Validações**: Revise os alertas de contas "Unknown" para adicionar classificações faltantes

## Contribuição

O projeto está organizado em módulos bem definidos. Para adicionar novos modelos de classificação:

1. Adicione o modelo em `pyaccount/core/account_classifier.py`
2. Adicione o tipo no Enum `TipoPlanoContas`
3. Registre no dicionário `MODELOS_CLASSIFICACAO`
4. Adicione testes em `test/core/account_classifier_test.py`

## Estrutura de Arquivos

```
pyaccount-project/
├── pyaccount/              # Módulo principal
│   ├── core/              # Funções base
│   ├── data/              # Acesso a dados
│   ├── builders/          # Construtores
│   └── export/            # Exportadores
├── test/                  # Testes organizados por módulo
├── config.ini             # Configuração (criar a partir de config.ini.example)
├── requirements.txt       # Dependências Python
├── sql_queries.sql        # Consultas SQL de referência
└── README.md              # Esta documentação
```

## Dependências

- `pandas` - Manipulação de dados
- `pyodbc` - Conexão ODBC com SQL Anywhere
- `openpyxl` - Geração de arquivos Excel
- `python-dateutil` - Parsing de datas

## Licença

[Adicione informações de licença aqui]
