# DQ AutoFix

> AI-powered repair suggestions for failed OpenMetadata Data Quality checks

[Python 3.14+](https://www.python.org/downloads/)
[FastAPI](https://fastapi.tiangolo.com/)
[Tests](#testing)
[License: MIT](LICENSE)

**Hackathon**: WeMakeDevs × OpenMetadata "Back to the Metadata" (Apr 17-26, 2026)  
**Track**: Paradox #T-02 — Data Observability  
**Issue**: [#26661 - Propose automated fixes for failed Data Quality checks](https://github.com/open-metadata/OpenMetadata/issues/26661)

---

## The Problem

When a Data Quality test fails in OpenMetadata, you get a notification that something is wrong — but **no guidance on how to fix it**. Engineers must:

1. Manually inspect the failing data
2. Figure out what went wrong (nulls? duplicates? format issues?)
3. Write SQL to fix it
4. Hope they don't break anything else

This is tedious, error-prone, and doesn't scale.

## The Solution

DQ AutoFix **automatically analyzes** failed DQ tests and **proposes fixes** with confidence scores:

```
DQ Test Fails in OpenMetadata
            ↓
DQ AutoFix fetches failure details + sample data
            ↓
Pattern detection (nulls, duplicates, whitespace, case issues)
            ↓
Strategy recommendation with confidence scoring
            ↓
Before/After preview + Copy-paste SQL + Rollback guards ✅
```

---

## Features


| Feature                  | Description                                                                             |
| ------------------------ | --------------------------------------------------------------------------------------- |
| **8 Fix Strategies**     | Mean/median/mode imputation, forward fill, trim whitespace, normalize case, deduplicate |
| **Confidence Scoring**   | 0-100% score based on data coverage, pattern clarity, reversibility, impact scope       |
| **Before/After Preview** | See exactly what will change before applying                                            |
| **Copy-Paste SQL**       | Ready-to-run fix SQL with syntax highlighting                                           |
| **Rollback Guards**      | Backup SQL generated for every fix                                                      |
| **Modern Web UI**        | Fast, responsive dashboard for reviewing fixes                                          |
| **REST API**             | Full API for integration with other tools                                               |


---

## Quick Start

### Prerequisites

- Python 3.14+
- [uv](https://docs.astral.sh/uv/) package manager
- Docker & Docker Compose (for OpenMetadata)

### 1. Start OpenMetadata

```bash
make docker-up
```

Wait 2-5 minutes for services to start. Access at [http://localhost:8585](http://localhost:8585)  
**Default login**: `admin@open-metadata.org` / `admin`

### 2. Get JWT Token

1. Login to OpenMetadata
2. Go to **Settings** → **Bots** → **ingestion-bot**
3. Click **Copy Token**

### 3. Configure & Run

```bash
# Install dependencies
make install

# Create .env from template
make env

# Edit .env and add your token
# OPENMETADATA_TOKEN=<your-jwt-token>

# Start the service
make dev
```

**Access points:**

- Web UI: [http://localhost:8000](http://localhost:8000)
- API Docs: [http://localhost:8000/docs](http://localhost:8000/docs)
- ReDoc: [http://localhost:8000/redoc](http://localhost:8000/redoc)

### 4. Create Some DQ Test Failures

In OpenMetadata:

1. Go to a table → **Profiler & Data Quality**
2. Add a test (e.g., `columnValuesToNotBeNull`)
3. Run the test suite
4. If tests fail, they'll appear in DQ AutoFix

---

## Fix Strategies


| Strategy              | Test Type                  | Confidence | Description                                          |
| --------------------- | -------------------------- | ---------- | ---------------------------------------------------- |
| **Mean Imputation**   | `columnValuesToNotBeNull`  | 60-85%     | Replace nulls with column mean                       |
| **Median Imputation** | `columnValuesToNotBeNull`  | 60-85%     | Replace nulls with column median (outlier-resistant) |
| **Mode Imputation**   | `columnValuesToNotBeNull`  | 65-80%     | Replace nulls with most frequent value               |
| **Forward Fill**      | `columnValuesToNotBeNull`  | 70-85%     | Fill nulls with previous non-null (time-series)      |
| **Trim Whitespace**   | `columnValuesToMatchRegex` | 95-100%    | Remove leading/trailing spaces (lossless)            |
| **Normalize Case**    | `columnValuesToBeInSet`    | 85-95%     | Convert to lower/upper/title case                    |
| **Keep First**        | `columnValuesToBeUnique`   | 75-90%     | Remove duplicates, keep first occurrence             |
| **Keep Last**         | `columnValuesToBeUnique`   | 75-90%     | Remove duplicates, keep last occurrence              |


### Confidence Scoring

Each recommendation includes a confidence score based on:


| Factor          | Weight | Description                      |
| --------------- | ------ | -------------------------------- |
| Data Coverage   | 25%    | How much data we can analyze     |
| Pattern Clarity | 25%    | How clear the failure pattern is |
| Reversibility   | 20%    | Can the fix be undone?           |
| Impact Scope    | 15%    | Percentage of rows affected      |
| Type Match      | 15%    | Does strategy match data type?   |


**Thresholds:**

- 🟢 **High** (≥80%): Safe to apply
- 🟡 **Medium** (60-80%): Review recommended
- 🟠 **Low** (40-60%): Use with caution
- ⚫ **Skip** (<40%): Not recommended

---

## API Reference


| Method | Endpoint                | Description                          |
| ------ | ----------------------- | ------------------------------------ |
| `GET`  | `/api/v1/health`        | Health check with version            |
| `GET`  | `/api/v1/failures`      | List all failed DQ tests             |
| `GET`  | `/api/v1/failures/{id}` | Get failure details                  |
| `POST` | `/api/v1/analyze`       | Analyze failure, get recommendations |
| `POST` | `/api/v1/suggest`       | Get best fix with SQL and preview    |
| `POST` | `/api/v1/preview`       | Preview a specific strategy          |
| `GET`  | `/api/v1/strategies`    | List available strategies            |


### Example: Get Fix Suggestion

```bash
curl -X POST http://localhost:8000/api/v1/suggest \
  -H "Content-Type: application/json" \
  -d '{"failureId": "customer_id_column_values_to_be_not_null_abc123"}'
```

**Response:**

```json
{
  "failureId": "customer_id_column_values_to_be_not_null_abc123",
  "strategy": "median_imputation",
  "strategyDescription": "Replace null values with the column median",
  "confidenceScore": 0.87,
  "confidenceBreakdown": {
    "data_coverage": 0.95,
    "pattern_clarity": 0.90,
    "reversibility": 0.50,
    "impact_scope": 0.98,
    "type_match": 1.0
  },
  "preview": {
    "beforeSample": [{"id": 1, "customer_id": null}],
    "afterSample": [{"id": 1, "customer_id": 45892}],
    "changesSummary": "Replace 127 null values with median 45892",
    "affectedRows": 127
  },
  "fixSql": "UPDATE customers SET customer_id = 45892 WHERE customer_id IS NULL;",
  "rollbackSql": "-- Backup created before fix\nCREATE TABLE customers_backup_20260419 AS SELECT * FROM customers WHERE customer_id IS NULL;"
}
```

---

## Architecture

```
openmetadata-dq-autofix/
├── src/dq_autofix/
│   ├── main.py                  # FastAPI app + static file serving
│   ├── config.py                # Pydantic Settings
│   ├── api/
│   │   ├── routes.py            # API endpoints
│   │   └── schemas.py           # Request/Response models
│   ├── analyzer/
│   │   ├── failure_analyzer.py  # Main orchestrator
│   │   ├── pattern_detector.py  # Pattern detection
│   │   └── sample_fetcher.py    # Data fetching
│   ├── strategies/
│   │   ├── base.py              # FixStrategy interface
│   │   ├── registry.py          # Strategy registry
│   │   ├── null_imputation.py   # Null handling strategies
│   │   ├── normalization.py     # Trim, case strategies
│   │   └── deduplication.py     # Dedupe strategies
│   ├── confidence/
│   │   └── scorer.py            # Confidence scoring
│   ├── preview/
│   │   ├── diff_generator.py    # Before/after diffs
│   │   ├── sql_generator.py     # SQL generation
│   │   └── rollback.py          # Rollback SQL
│   └── openmetadata/
│       ├── client.py            # OM API client
│       └── models.py            # Data models
├── static/                      # Web UI
│   ├── index.html
│   ├── styles.css
│   └── app.js
├── tests/                       # 249 tests
├── docker-compose.yml           # OpenMetadata stack
└── Makefile                     # Dev commands
```

---

## Development

### Commands

```bash
make install      # Install dependencies
make dev          # Run with auto-reload
make test         # Run all tests
make test-cov     # Tests with coverage
make lint         # Check code style
make format       # Format code
make typecheck    # Type check with mypy
make docker-up    # Start OpenMetadata
make docker-down  # Stop OpenMetadata
```

### Testing

```bash
# Run all 249 tests
make test

# Run specific test file
uv run pytest tests/test_integration.py -v

# Run with coverage
make test-cov
```


| Test Suite  | Tests   | Coverage                    |
| ----------- | ------- | --------------------------- |
| Strategies  | 77      | null, trim, case, dedupe    |
| Analyzer    | 70      | pattern detection, fetching |
| Confidence  | 26      | scoring logic               |
| Preview     | 17      | diff, SQL generation        |
| API         | 17      | endpoints                   |
| Integration | 26      | end-to-end                  |
| Client      | 8       | OM API client               |
| **Total**   | **249** | -                           |


---

## Configuration


| Variable             | Default                 | Description          |
| -------------------- | ----------------------- | -------------------- |
| `OPENMETADATA_HOST`  | `http://localhost:8585` | OpenMetadata URL     |
| `OPENMETADATA_TOKEN` | -                       | JWT token (required) |
| `LOG_LEVEL`          | `INFO`                  | Logging level        |


---

## Tech Stack

- **Backend**: FastAPI, Pydantic v2, httpx (async)
- **Frontend**: Vanilla JS, CSS (no frameworks)
- **Package Manager**: uv
- **Testing**: pytest, pytest-asyncio (249 tests)
- **Linting**: Ruff (strict mode)
- **Type Checking**: mypy (strict mode)
- **Infrastructure**: Docker Compose, OpenMetadata 1.6.2

---

## Links

- [OpenMetadata](https://open-metadata.org/)
- [OpenMetadata Docs](https://docs.open-metadata.org/)
- [Hackathon Issue #26661](https://github.com/open-metadata/OpenMetadata/issues/26661)
- [WeMakeDevs](https://wemakedevs.org/)

---

## License

MIT