# Local warehouse backfill verification

Issue: [#37](https://github.com/NicollMitchManingat/JowenTwoThree-ETL-Pipeline/issues/37)

The hosted warehouse proposal was cancelled. Live reporting uses the existing local PostgreSQL database `jowen_analytics` at `127.0.0.1:55432`, with the independent restricted `jowen_etl_local` login from the gitignored `.env.warehouse.local`. Demo and fault tests use `jowen_analytics_demo`.

Verified on November 11, 2026, in Asia/Manila. Read-only source queries found reporting history from July 15 through November 10, with no current-day/future records or missing timestamps in the three dated source tables. The full inclusive range was freshly extracted, transformed, approved and published locally. Inventory was captured on November 11 at 22:32 Philippine time.

| Source dataset | Extracted rows | Excluded | Rejected |
| --- | ---: | ---: | ---: |
| Transactions | 252 | 0 | 0 |
| Refunds | 1 | 0 | 0 |
| Customer traffic | 221 | 0 | 0 |
| Inventory | 29 | 0 | 0 |

The warehouse contains 119 daily sales rows, 119 daily traffic rows, one current inventory snapshot and 120 date dimension rows. It stores reporting summaries; immutable bronze packages retain the extracted source records.

Independent SQL queries against the read-only source and the local warehouse agreed on sales of 79,245.50, refunds of 450.00, net sales of 78,795.50, 252 paid orders and 1,456 customers. The inventory snapshot contains 29 active items, three low-stock items and no out-of-stock items. All loaded values also passed comparison against the approved silver frames.

The load succeeded and replaying the exact approved manifest returned `SKIPPED`, with zero inserts/updates and one success-ledger row for that batch. A pre-load local database backup was created. The source connection confirmed `transaction_read_only=on`; no POS writes or application edits were performed.

Local verification artifacts (gitignored):

- Completion: `data/handover/Pipeline_20261111_143212_d2cebd49f8ae/completion.json`
- Bronze: `data/bronze/Run_20261111_143213_a2ae80c8fcbf/manifest.json`
- Silver: `data/silver/Transform_20261111_143214_e32d4bd89548/manifest.json`
- Independent checks: `data/verification/local_backfill_verified.json`

Commands used:

~~~powershell
.\.venv\Scripts\python.exe -B tools/local_warehouse.py --backup
.\.venv\Scripts\python.exe -B -m pipelines.run --start-date 2026-07-15 --end-date 2026-11-10
.\.venv\Scripts\python.exe -B -m pipelines.load.run --manifest data/silver/Transform_20261111_143214_e32d4bd89548/manifest.json
$env:WAREHOUSE_TEST_DATABASE = 'jowen_analytics_demo'
.\.venv\Scripts\python.exe -B -m pytest -p no:cacheprovider -o addopts='' -q
.\.venv\Scripts\python.exe -B -m ruff check .
~~~

Validation: 158 tests passed, including native PostgreSQL rollback, replay, stale-snapshot and financial-corruption checks; lint passed. The daily wrapper starts the local warehouse before executing the pipeline. No scheduled task was registered during this change.
