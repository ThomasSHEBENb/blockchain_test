import logging
import os
from decimal import Decimal

import asyncpg

from src.config import settings

log = logging.getLogger(__name__)

_pool = None


async def get_pool():
    global _pool
    if _pool is None:
        log.info(f"connecting to pg: {settings.db_host}:{settings.db_port}/{settings.db_name}")
        _pool = await asyncpg.create_pool(
            host=settings.db_host,
            port=settings.db_port,
            user=settings.db_user,
            password=settings.db_password,
            database=settings.db_name,
            min_size=2, max_size=10,
        )
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def init_db():
    pool = await get_pool()
    sql_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "sql", "init.sql")
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"не найден init.sql: {sql_path}")

    with open(sql_path, "r", encoding="utf-8") as f:
        ddl = f.read()
    async with pool.acquire() as conn:
        await conn.execute(ddl)
    log.info("schema ok")


async def get_last_indexed_block(contract_addr, default_block):
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT last_indexed_block FROM indexing_state WHERE LOWER(contract_address) = LOWER($1)",
        contract_addr.lower()
    )
    if row and row["last_indexed_block"] is not None:
        return int(row["last_indexed_block"])
    return default_block


async def save_transfers_and_checkpoint(transfers, contract_addr, last_block):
    pool = await get_pool()

    insert_q = """
        INSERT INTO token_transfers (
            tx_hash, block_number, block_timestamp, contract_address,
            token_standard, token_id, from_address, to_address, amount, log_index
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT (tx_hash, contract_address, token_id, from_address, to_address, amount, log_index)
        DO NOTHING
    """

    checkpoint_q = """
        INSERT INTO indexing_state (contract_address, last_indexed_block, updated_at)
        VALUES (LOWER($1), $2, NOW())
        ON CONFLICT (contract_address)
        DO UPDATE SET last_indexed_block = EXCLUDED.last_indexed_block, updated_at = NOW()
    """

    async with pool.acquire() as conn:
        async with conn.transaction():
            if transfers:
                rows = []
                for t in transfers:
                    tid = Decimal(str(t["token_id"])) if t.get("token_id") is not None else None
                    rows.append((
                        t["tx_hash"],
                        t["block_number"],
                        t.get("block_timestamp"),
                        t["contract_address"].lower(),
                        t["token_standard"],
                        tid,
                        t["from_address"].lower(),
                        t["to_address"].lower(),
                        Decimal(str(t["amount"])),
                        t.get("log_index", 0),
                    ))
                await conn.executemany(insert_q, rows)
            await conn.execute(checkpoint_q, contract_addr.lower(), last_block)


async def get_wallet_balances(wallet):
    pool = await get_pool()
    rows = await pool.fetch("""
        SELECT contract_address, token_standard, token_id, calculated_balance
        FROM v_wallet_balances
        WHERE LOWER(wallet_address) = LOWER($1)
        ORDER BY contract_address, token_id NULLS FIRST
    """, wallet.lower())
    result = []
    for r in rows:
        result.append({
            "contract_address": r["contract_address"],
            "token_standard": r["token_standard"],
            "token_id": int(r["token_id"]) if r["token_id"] is not None else None,
            "calculated_balance": int(r["calculated_balance"]),
        })
    return result
