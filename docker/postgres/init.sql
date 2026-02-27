CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Tabela que o Spark vai popular
CREATE TABLE IF NOT EXISTS silver.gastos (
    orgao_superior  TEXT,
    orgao           TEXT NOT NULL,
    valor_pago      NUMERIC(15, 2),
    data_referencia DATE,
    ano_referencia  INTEGER
);