import argparse
import asyncio
import logging
import sys

from rich.logging import RichHandler

from src.config import settings
from src.db import close_pool, init_db
from src.indexer import Indexer
from src.reconciler import reconcile
from src.rpc_client import RPCClient


def setup_log(level):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(message)s", datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, show_path=False)],
    )


async def run(args):
    setup_log(args.log_level)
    logger = logging.getLogger("main")

    wallet = args.wallet.lower()
    logger.info(f"wallet: {wallet}")

    # бд
    try:
        await init_db()
    except Exception as e:
        logger.error(f"pg connection failed: {e}")
        return 1

    try:
        async with RPCClient(rpc_urls=settings.polygon_rpc_urls) as rpc:
            if not args.reconcile_only:
                logger.info("запускаем индексацию...")
                idx = Indexer(
                    wallet=wallet, rpc=rpc,
                    start_block=args.start_block,
                    chunk_size=args.chunk_size,
                )
                await idx.run()

            logger.info("запускаем сверку балансов...")
            ok = await reconcile(wallet, rpc)
            return 0 if ok else 1

    except Exception as e:
        logger.exception(f"fatal: {e}")
        return 1
    finally:
        await close_pool()


def main():
    p = argparse.ArgumentParser(description="Polymarket wallet indexer + balance reconciler")
    p.add_argument("--wallet", default=settings.target_wallet,
                    help=f"целевой кошелёк (default: {settings.target_wallet})")
    p.add_argument("--start-block", type=int, default=settings.start_block)
    p.add_argument("--chunk-size", type=int, default=settings.chunk_size)
    p.add_argument("--reconcile-only", action="store_true",
                    help="пропустить индексацию, только сверка")
    p.add_argument("--log-level", default=settings.log_level,
                    choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = p.parse_args()

    rc = asyncio.run(run(args))
    sys.exit(rc)


if __name__ == "__main__":
    main()
