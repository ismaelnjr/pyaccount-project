# Beancount SQL Pipeline (Betha/SQL Anywhere via ODBC)

Gera um arquivo `.beancount` a partir de consultas SQL (plano de contas, saldos de abertura e lançamentos por período).

## Estrutura
- `beancount_pipeline.py` — script principal (CLI)
- `sql_queries.sql` — consultas de referência (usadas inline no script)
- `requirements.txt` — dependências
- `config.ini.example` — exemplo de configuração

## Instalação
```bash
python -m venv .venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
```

> **ODBC**: configure um DSN para SQL Anywhere (ex.: `SQLANYWHERE17`).

## Uso (exemplos)

### 1) Via argumentos
```bash
python beancount_pipeline.py   --dsn SQLANYWHERE17 --user dba --password sql   --empresa 437 --inicio 2025-09-01 --fim 2025-09-18   --saida ./out --moeda BRL --somente-ativas
```

### 2) Com arquivo de configuração
Crie um `config.ini` a partir do `config.ini.example`, e execute:
```bash
python beancount_pipeline.py   --config ./config.ini   --empresa 437 --inicio 2025-09-01 --fim 2025-09-18   --saida ./out
```

## Saídas
- `out/lancamentos_<empresa>_<inicio>_<fim>.beancount`
- `out/mapa_beancount_<empresa>.csv` (CLAS_CTA → Conta Beancount)
- `out/balancete_abertura_<empresa>_<D-1>.csv` (para conferência)

## Notas de integridade
- O script alerta se a **soma dos saldos de abertura** (débitos − créditos) não ≈ 0.
- Checa indícios de inconsistência por `nume_lan` (linhas ímpares, contas sem mapa).

## Dicas
- Ajuste o mapeamento `classificar_beancount()` caso o seu `TIPO_CTA` possua codificação diferente.
- Se houver **tabela oficial de saldos** (mensais/acumulados), substitua a query de saldos para usar essa base.
- Para conciliação, você pode inserir diretivas `balance` manualmente nas contas‑chave no `.beancount`.


## Cache de Saldos Iniciais (opcional e recomendado)
Para evitar varredura histórica a cada execução, gere um CSV de **saldos iniciais** até a data D‑1 do período que você quer abrir:

```bash
python build_opening_balances.py   --dsn SQLANYWHERE17 --user dba --password sql   --empresa 437 --ate 2025-08-31   --saida ./out
```

Depois, passe o CSV ao pipeline principal:

```bash
python beancount_pipeline.py   --config ./config.ini   --empresa 437 --inicio 2025-09-01 --fim 2025-09-18   --saldos ./out/saldos_iniciais_437_2025-08-31.csv   --saida ./out
```
**Observações**
- O CSV contém ao menos `BC_ACCOUNT` e `saldo`. Colunas extras (`empresa`, `data_corte`) são usadas para alerta/validação.
- Se `data_corte` != D‑1 do período, o script apenas alerta (casos de abertura com defasagem).
- Recrie o cache sempre que houver lançamentos retroativos anteriores à data de corte.
