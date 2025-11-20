-- Tabela Staging: recebe dados brutos do CSV sem transformação
-- Essa é a camada Bronze (ingestão crua)
-- Será truncada a cada execução do pipeline
CREATE TABLE IF NOT EXISTS staging (
    date DATE,                  -- Data da cotação
    symbol VARCHAR(10),         -- Símbolo do ativo (ticker)
    open NUMERIC,               -- Preço de abertura do dia
    high NUMERIC,               -- Preço máximo do dia
    low NUMERIC,                -- Preço mínimo do dia
    close NUMERIC,              -- Preço de fechamento do dia
    volume BIGINT               -- Volume total de operações
);