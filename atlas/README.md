# ATLAS Sprint 0 — BTC-PERP Data Collection

## Quick Start

```bash
cd atlas
cp .env.example .env
docker compose up -d
```

## Services
| Service | Port | Purpose |
|---|---|---|
| TimescaleDB | 5432 | Market data hypertables |
| Redis | 6379 | Event bus (Streams) |
| App | 8000 | Ingest + /health + /metrics |
| Grafana | 3001 | Dashboard (admin/admin) |

## Acceptance Test (after 24h)
```sql
-- Run in: docker compose exec db psql -U atlas
SELECT
  count(*) AS total_trades,
  min(ts_exchange) AS first,
  max(ts_exchange) AS last,
  (SELECT count(*) FROM data_gaps WHERE resolved=false) AS open_gaps
FROM trades WHERE symbol='BTC-PERP';
```
Target: `open_gaps = 0`, continuous data for 24h → **Sprint 0 complete**.

## No API key needed for Sprint 0
All streams (aggTrade, depth, forceOrder, OI, funding) are public.

## Tests
```bash
pip install -e ".[dev]"
pytest tests/ -v  # 11/11 expected
```
