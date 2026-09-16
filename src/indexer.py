import asyncio
import logging
from datetime import datetime, timezone

from rich.progress import (
    BarColumn, MofNCompleteColumn, Progress, SpinnerColumn,
    TaskProgressColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn,
)

from src.config import settings
from src.db import get_last_indexed_block, save_transfers_and_checkpoint
from src.rpc_client import RPCClient

log = logging.getLogger(__name__)

ZERO_ADDR = "0x" + "0" * 40


def _pad(addr):
    return "0x" + addr.lower().replace("0x", "").zfill(64)


def _unpad(topic):
    return "0x" + topic.lower().replace("0x", "")[-40:]


def decode_batch_data(hex_data):
    """декодим abi-encoded uint256[] ids, uint256[] values из TransferBatch"""
    raw = hex_data[2:] if hex_data.startswith("0x") else hex_data

    off_ids = int(raw[0:64], 16) * 2  # offset в hex chars
    off_vals = int(raw[64:128], 16) * 2

    n_ids = int(raw[off_ids:off_ids + 64], 16)
    ids = []
    pos = off_ids + 64
    for _ in range(n_ids):
        ids.append(int(raw[pos:pos + 64], 16))
        pos += 64

    n_vals = int(raw[off_vals:off_vals + 64], 16)
    vals = []
    pos = off_vals + 64
    for _ in range(n_vals):
        vals.append(int(raw[pos:pos + 64], 16))
        pos += 64

    return list(zip(ids, vals))


def parse_log(log_entry, standard):
    """превращаем сырой лог в список нормализованных transfer записей"""
    tx = log_entry["transactionHash"]
    blk = int(log_entry["blockNumber"], 16)
    addr = log_entry["address"].lower()
    topics = log_entry.get("topics", [])
    data = log_entry.get("data", "0x")
    log_idx = int(log_entry.get("logIndex", "0x0"), 16)

    # timestamp может быть, а может и нет
    ts = None
    if log_entry.get("blockTimestamp"):
        ts = datetime.fromtimestamp(int(log_entry["blockTimestamp"], 16), tz=timezone.utc)
        ts = ts.replace(tzinfo=None)  # pg не любит tz aware

    transfers = []

    if standard == "ERC20":
        if len(topics) >= 3 and topics[0].lower() == settings.topic_erc20_transfer.lower():
            val = int(data, 16) if data and data != "0x" else 0
            transfers.append({
                "tx_hash": tx, "block_number": blk, "block_timestamp": ts,
                "contract_address": addr, "token_standard": "ERC20",
                "token_id": None,
                "from_address": _unpad(topics[1]), "to_address": _unpad(topics[2]),
                "amount": val, "log_index": log_idx,
            })

    elif standard == "ERC1155":
        t0 = topics[0].lower() if topics else ""

        if t0 == settings.topic_erc1155_transfer_single.lower() and len(topics) >= 4:
            raw = data[2:] if data.startswith("0x") else data
            token_id = int(raw[0:64], 16) if len(raw) >= 64 else 0
            amount = int(raw[64:128], 16) if len(raw) >= 128 else 0
            transfers.append({
                "tx_hash": tx, "block_number": blk, "block_timestamp": ts,
                "contract_address": addr, "token_standard": "ERC1155",
                "token_id": token_id,
                "from_address": _unpad(topics[2]), "to_address": _unpad(topics[3]),
                "amount": amount, "log_index": log_idx,
            })

        elif t0 == settings.topic_erc1155_transfer_batch.lower() and len(topics) >= 4:
            pairs = decode_batch_data(data)
            for i, (tid, amt) in enumerate(pairs):
                transfers.append({
                    "tx_hash": tx, "block_number": blk, "block_timestamp": ts,
                    "contract_address": addr, "token_standard": "ERC1155",
                    "token_id": tid,
                    "from_address": _unpad(topics[2]), "to_address": _unpad(topics[3]),
                    "amount": amt,
                    "log_index": log_idx * 1000 + i,  # чтобы уникальный constraint не ругался
                })

    return transfers


async def _find_first_activity(rpc, contract, standard, wallet, min_blk, max_blk):
    """бинарный(ну почти) поиск первого блока где кошелек начал активность"""
    padded = _pad(wallet)

    if standard == "ERC20":
        queries = [
            [settings.topic_erc20_transfer, None, padded],
            [settings.topic_erc20_transfer, padded, None],
        ]
    else:
        queries = [
            [settings.topic_erc1155_transfer_single, None, None, padded],
            [settings.topic_erc1155_transfer_single, None, padded, None],
            [settings.topic_erc1155_transfer_batch, None, None, padded],
            [settings.topic_erc1155_transfer_batch, None, padded, None],
        ]

    step = 5_000_000
    for start in range(min_blk, max_blk, step):
        end = min(start + step - 1, max_blk)
        for q in queries:
            try:
                logs = await rpc.get_logs(contract, start, end, q)
                if logs:
                    first = min(int(l["blockNumber"], 16) for l in logs)
                    log.info(f"first activity for {contract}: block {first}")
                    return max(min_blk, first - 1000)
            except Exception:
                continue
    return min_blk


class Indexer:
    def __init__(self, wallet, rpc, start_block=None, chunk_size=None, concurrency=8):
        self.wallet = wallet.lower()
        self.padded = _pad(wallet)
        self.rpc = rpc
        self.start_block = start_block or settings.start_block
        self.chunk = chunk_size or settings.chunk_size
        self.concurrency = concurrency

        self.contracts = [
            {"name": "USDC.e",            "addr": settings.usdc_e_address.lower(),     "std": "ERC20"},
            {"name": "USDC (native)",     "addr": settings.native_usdc_address.lower(),"std": "ERC20"},
            {"name": "CTF (ERC1155)",     "addr": settings.ctf_address.lower(),        "std": "ERC1155"},
        ]

    async def run(self):
        head = await self.rpc.get_block_number()
        log.info(f"current polygon block: {head}")

        for c in self.contracts:
            name, addr, std = c["name"], c["addr"], c["std"]
            log.info(f"=== {name} ({addr}) ===")

            last = await get_last_indexed_block(addr, self.start_block)

            # пропускаем пустые блоки в начале если ещё не индексировали
            if last <= 13_000_000:
                log.info(f"ищем первую активность для {name}...")
                detected = await _find_first_activity(self.rpc, addr, std, self.wallet, 13_000_000, head)
                last = max(last, detected)

            if last >= head:
                log.info(f"{name} уже синхронизирован до {last}")
                continue

            await self._do_index(name, addr, std, last, head)

    async def _do_index(self, name, contract, standard, from_blk, to_blk):
        total = to_blk - from_blk + 1
        cursor = from_blk

        # собираем topic фильтры
        if standard == "ERC20":
            filters = [
                [settings.topic_erc20_transfer, None, self.padded],
                [settings.topic_erc20_transfer, self.padded, None],
            ]
        else:
            filters = [
                [settings.topic_erc1155_transfer_single, None, None, self.padded],
                [settings.topic_erc1155_transfer_single, None, self.padded, None],
                [settings.topic_erc1155_transfer_batch, None, None, self.padded],
                [settings.topic_erc1155_transfer_batch, None, self.padded, None],
            ]

        with Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
            BarColumn(), TaskProgressColumn(), MofNCompleteColumn(),
            TimeElapsedColumn(), TimeRemainingColumn(),
        ) as prog:
            task = prog.add_task(f"indexing {name}", total=total)

            while cursor <= to_blk:
                chunks = []
                batch_start = cursor
                for _ in range(self.concurrency):
                    if cursor > to_blk:
                        break
                    cf = cursor
                    ct = min(cf + self.chunk - 1, to_blk)
                    chunks.append((cf, ct))
                    cursor = ct + 1

                if not chunks:
                    break

                batch_end = chunks[-1][1]

                # параллельно запрашиваем логи по всем чанкам и фильтрам
                tasks = []
                for cf, ct in chunks:
                    for f in filters:
                        tasks.append(self.rpc.get_logs(contract, cf, ct, f))

                results = await asyncio.gather(*tasks)

                # дедупликация по tx+logIndex
                seen = {}
                for log_list in results:
                    for entry in log_list:
                        k = (entry["transactionHash"].lower(), int(entry.get("logIndex", "0x0"), 16))
                        seen[k] = entry

                # парсим
                parsed = []
                for entry in seen.values():
                    parsed.extend(parse_log(entry, standard))

                await save_transfers_and_checkpoint(parsed, contract, batch_end)
                prog.update(task, advance=batch_end - batch_start + 1)

        log.info(f"done: {name} indexed to block {to_blk}")
