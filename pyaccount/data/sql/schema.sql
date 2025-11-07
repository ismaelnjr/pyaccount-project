CREATE TABLE IF NOT EXISTS empresas (
  CODI_EMP INTEGER PRIMARY KEY,
  NOME TEXT NOT NULL
);

-- Insere empresa inicial se não existir
INSERT OR IGNORE INTO empresas (CODI_EMP, NOME) VALUES (267, 'OC ALIMENTOS');

CREATE TABLE IF NOT EXISTS plano_contas (
  codi_emp INTEGER NOT NULL,
  codi_cta TEXT NOT NULL,
  nome_cta TEXT,
  clas_cta TEXT,
  tipo_cta TEXT,
  situacao_cta TEXT,
  bc_group TEXT,
  PRIMARY KEY (codi_emp, codi_cta)
);

CREATE TABLE IF NOT EXISTS saldos_iniciais (
  codi_emp INTEGER NOT NULL,
  conta TEXT NOT NULL,
  data_saldo DATE NOT NULL,
  saldo NUMERIC NOT NULL,
  PRIMARY KEY (codi_emp, conta, data_saldo)
);

CREATE TABLE IF NOT EXISTS lancamentos (
  codi_emp INTEGER NOT NULL,
  nume_lan INTEGER NOT NULL,
  data_lan DATE NOT NULL,
  codi_lote INTEGER,
  tipo_lote TEXT,
  codi_his TEXT,
  chis_lan TEXT,
  ndoc_lan TEXT,
  codi_usu TEXT,
  lado TEXT CHECK(lado IN ('D','C')) NOT NULL,
  conta TEXT NOT NULL,
  valor NUMERIC NOT NULL,
  PRIMARY KEY (codi_emp, nume_lan, lado, conta, data_lan)
);

CREATE INDEX IF NOT EXISTS ix_lanc_data  ON lancamentos(codi_emp, data_lan);
CREATE INDEX IF NOT EXISTS ix_lanc_conta ON lancamentos(codi_emp, conta);
CREATE INDEX IF NOT EXISTS ix_si_ref     ON saldos_iniciais(codi_emp, conta, data_saldo);
