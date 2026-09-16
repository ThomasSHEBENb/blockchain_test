-- чекпоинт индексации
CREATE TABLE IF NOT EXISTS indexing_state (
    contract_address VARCHAR(42) PRIMARY KEY,
    last_indexed_block BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- все трансферы
CREATE TABLE IF NOT EXISTS token_transfers (
    id BIGSERIAL PRIMARY KEY,
    tx_hash VARCHAR(66) NOT NULL,
    block_number BIGINT NOT NULL,
    block_timestamp TIMESTAMP NULL,
    contract_address VARCHAR(42) NOT NULL,
    token_standard VARCHAR(10) NOT NULL,
    token_id NUMERIC(78, 0) NULL,  -- null для erc20, uint256 для erc1155
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42) NOT NULL,
    amount NUMERIC(78, 0) NOT NULL,
    log_index INT NOT NULL DEFAULT 0,
    CONSTRAINT uq_token_transfers UNIQUE NULLS NOT DISTINCT (
        tx_hash, contract_address, token_id, from_address, to_address, amount, log_index
    )
);

CREATE INDEX IF NOT EXISTS idx_transfers_to ON token_transfers (to_address);
CREATE INDEX IF NOT EXISTS idx_transfers_from ON token_transfers (from_address);
CREATE INDEX IF NOT EXISTS idx_transfers_contract_blk ON token_transfers (contract_address, block_number);
CREATE INDEX IF NOT EXISTS idx_transfers_txhash ON token_transfers (tx_hash);

-- view для расчёта балансов: incoming - outgoing, только > 0
CREATE OR REPLACE VIEW v_wallet_balances AS
SELECT
    wallet_address, contract_address, token_standard, token_id,
    SUM(delta) AS calculated_balance
FROM (
    SELECT to_address AS wallet_address, contract_address, token_standard, token_id, amount AS delta
    FROM token_transfers
    UNION ALL
    SELECT from_address AS wallet_address, contract_address, token_standard, token_id, -amount AS delta
    FROM token_transfers
) t
GROUP BY wallet_address, contract_address, token_standard, token_id
HAVING SUM(delta) > 0;
