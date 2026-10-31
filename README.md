# JowenTwoThree ETL Pipeline

Batch ETL pipeline for the Jowen Two Three web application (POS, Inventory Management, and Customer Traffic Recording System for Jowen's Kitchen & Cafe).

Source: Supabase PostgreSQL operational database → Python daily batch extraction → PostgreSQL data warehouse for daily, weekly, and monthly business reporting.

## Table of Contents

- [Stage 1: Data Extraction](docs/extraction/stage-1-data-extraction.md)
- [Scheduling the Daily Extraction Batch](docs/extraction/scheduling.md)
- [Stage 2: Data Transformation](docs/transformation/stage-2-data-transformation.md)

## Setup

Requires Python 3.11.

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

The example environment enables mock mode (`ETL_MOCK_MODE=true`), which uses
deterministic sample data and needs no database connection. Set
`ETL_MOCK_MODE=false` and fill in the `SUPABASE_DB_*` values to read from the
real source database.

## Testing

```powershell
python -m pytest -q
python -m ruff check .
```
