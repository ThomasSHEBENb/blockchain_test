import asyncio
import logging
import random

import httpx

from src.config import settings

log = logging.getLogger(__name__)


class RPCClient:
    """json-rpc клиент для polygon с ротацией эндпоинтов"""

    def __init__(self, rpc_urls=None, timeout=20.0):
        self.urls = [u.rstrip("/") for u in (rpc_urls or settings.polygon_rpc_urls)]
        assert self.urls, "нужна хотя бы одна rpc url"
        self._idx = 0
        self.timeout = timeout
        self._client = None
        self._req_id = 0

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout, connect=10.0),
            headers={"Content-Type": "application/json"},
        )
        return self

    async def __aexit__(self, *exc):
        if self._client:
            await self._client.aclose()
            self._client = None

    @property
    def url(self):
        return self.urls[self._idx]

    def _rotate(self, why=""):
        old = self.url
        self._idx = (self._idx + 1) % len(self.urls)
        log.warning(f"rpc rotate ({why}): {old} -> {self.url}")

    async def _call(self, method, params):
        if not self._client:
            raise RuntimeError("use async with!")

        self._req_id += 1
        payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": self._req_id}

        max_attempts = settings.max_retries * len(self.urls)
        backoff = settings.initial_backoff

        for attempt in range(max_attempts):
            ep = self.url
            try:
                resp = await self._client.post(ep, json=payload)

                if resp.status_code in (429, 503):
                    wait = backoff + random.uniform(0.1, 0.5)
                    log.warning(f"rate limited {resp.status_code} on {ep}, sleep {wait:.1f}s")
                    await asyncio.sleep(wait)
                    backoff = min(backoff * 2, 15)
                    self._rotate(f"http {resp.status_code}")
                    continue

                if resp.status_code in (401, 403):
                    self._rotate(f"http {resp.status_code}")
                    continue

                if resp.status_code != 200:
                    self._rotate(f"http {resp.status_code}")
                    continue

                data = resp.json()

                if "error" in data:
                    err = data["error"]
                    msg = str(err.get("message", err)).lower()
                    code = err.get("code") if isinstance(err, dict) else None

                    if code in (-32005, -32029, -32016) or "rate limit" in msg or "too many" in msg:
                        wait = backoff + random.uniform(0.1, 0.5)
                        log.warning(f"rpc rate limit: {msg}, waiting {wait:.1f}s")
                        await asyncio.sleep(wait)
                        backoff = min(backoff * 2, 15)
                        self._rotate("rate limit")
                        continue

                    if any(x in msg for x in ("unauthorized", "api key", "tenant disabled")):
                        self._rotate("auth")
                        continue

                    if "pruned" in msg or "historical" in msg:
                        self._rotate("pruned node")
                        continue

                    # если нода жалуется на диапазон — пусть вызывающий код разбирается
                    if "range" in msg or "10000 blocks" in msg or "query returned more" in msg:
                        raise ValueError(f"RANGE_LIMIT: {msg}")

                    log.error(f"rpc error {ep}: {err}")
                    self._rotate(f"error {code}")
                    continue

                return data.get("result")

            except (httpx.TimeoutException, httpx.NetworkError) as e:
                wait = min(backoff, 5) + random.uniform(0.1, 0.3)
                log.warning(f"network err {type(e).__name__} on {ep}, retry in {wait:.1f}s")
                await asyncio.sleep(wait)
                backoff = min(backoff * 1.5, 10)
                self._rotate(type(e).__name__)
            except ValueError:
                raise  # range limit пробрасываем

        raise RuntimeError(f"все rpc ноды упали после {max_attempts} попыток ({method})")

    async def get_block_number(self):
        res = await self._call("eth_blockNumber", [])
        return int(res, 16)

    async def eth_call(self, to, data, block="latest"):
        r = await self._call("eth_call", [{"to": to.lower(), "data": data}, block])
        return str(r or "0x0")

    async def get_logs(self, address, from_block, to_block, topics):
        params = [{
            "address": address.lower(),
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "topics": topics,
        }]
        try:
            res = await self._call("eth_getLogs", params)
            return res or []
        except ValueError as e:
            if "RANGE_LIMIT" in str(e) and to_block > from_block:
                mid = (from_block + to_block) // 2
                log.info(f"splitting range [{from_block}..{to_block}] -> [{from_block}..{mid}] + [{mid+1}..{to_block}]")
                a = await self.get_logs(address, from_block, mid, topics)
                b = await self.get_logs(address, mid + 1, to_block, topics)
                return a + b
            raise

    async def balance_of_erc20(self, contract, wallet, block="latest"):
        addr = wallet.lower().replace("0x", "").zfill(64)
        raw = await self.eth_call(contract, f"0x70a08231{addr}", block)
        return int(raw, 16) if raw.startswith("0x") and len(raw) > 2 else 0

    async def balance_of_erc1155(self, contract, wallet, token_id, block="latest"):
        addr = wallet.lower().replace("0x", "").zfill(64)
        tid = hex(token_id)[2:].zfill(64)
        raw = await self.eth_call(contract, f"0x00fdd58e{addr}{tid}", block)
        return int(raw, 16) if raw.startswith("0x") and len(raw) > 2 else 0
