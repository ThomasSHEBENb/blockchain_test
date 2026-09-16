# Polymarket Wallet Indexer

Индексация истории кошелька на Polymarket через Polygon JSON-RPC и сверка балансов.

Кошелек: `0x46b353667fd7d846af3bbeda6584b0e5b883d3de`

## Что делает

- Парсит `eth_getLogs` напрямую с публичных Polygon RPC нод (без API Polymarket, без The Graph и т.п.)
- Индексирует ERC-20 трансферы (USDC.e + native USDC) и ERC-1155 (ConditionalTokens)
- Декодирует TransferSingle и TransferBatch (включая динамические массивы ids/values)
- Сохраняет в PostgreSQL с дедупликацией и чекпоинтами
- Сверяет рассчитанные балансы с реальным состоянием контрактов через `eth_call`

## Структура

```
├── sql/init.sql          - DDL: таблицы, индексы, view для балансов
├── src/
│   ├── config.py         - конфиг через pydantic-settings + .env
│   ├── db.py             - asyncpg, пул соединений, CRUD
│   ├── rpc_client.py     - json-rpc клиент с ротацией нод и backoff
│   ├── indexer.py        - индексация трансферов
│   ├── reconciler.py     - сверка балансов db vs on-chain
│   └── main.py           - CLI entry point
├── .env                  - настройки (бд, rpc urls, кошелёк)
├── docker-compose.yml    - postgres в docker (опционально)
└── requirements.txt
```

## Запуск

### Предварительно

1. PostgreSQL 16+ (локальный или через docker)
2. Python 3.11+
3. `pip install -r requirements.txt`

### Настройка .env

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=polymarket_indexer
DB_USER=postgres
DB_PASSWORD=admin123

POLYGON_RPC_URLS=https://gateway.tenderly.co/public/polygon,https://polygon-bor-rpc.publicnode.com,https://polygon.drpc.org
TARGET_WALLET=0x46b353667fd7d846af3bbeda6584b0e5b883d3de
```

### Запуск индексации + сверки

```bash
python -m src.main
```

Или с параметрами:
```bash
python -m src.main --wallet 0x46b353667fd7d846af3bbeda6584b0e5b883d3de --chunk-size 3000
```

Только сверка (если данные уже есть в бд):
```bash
python -m src.main --reconcile-only
```

### Docker (postgres)

```bash
docker-compose up -d
python -m src.main
```

## Как работает сверка

Для каждого токена в бд:
- ERC-20: вызывает `balanceOf(address)` через `eth_call`
- ERC-1155: вызывает `balanceOf(address, uint256)` через `eth_call`

Сравнивает с рассчитанным балансом из view `v_wallet_balances` (сумма входящих минус сумма исходящих).

Если все дельты = 0, значит индексация корректная.

## Схема бд

- `indexing_state` — чекпоинт (какой блок последний обработан для каждого контракта)
- `token_transfers` — все трансферы с дедупликацией по `(tx_hash, contract_address, token_id, from, to, amount, log_index)`
- `v_wallet_balances` — view, считает баланс как `SUM(incoming) - SUM(outgoing)`, фильтрует `> 0`

## CLI

```
--wallet          адрес кошелька (default из .env)
--start-block     с какого блока начать (default: 80000000)
--chunk-size      размер чанка для eth_getLogs (default: 3000)
--reconcile-only  только сверка без индексации
--log-level       DEBUG/INFO/WARNING/ERROR
```
