# OpenMetadata DQ AutoFix

AI-powered repair suggestions for failed OpenMetadata Data Quality checks.

**Hackathon**: WeMakeDevs "Back to the Metadata" (Apr 17-26, 2026)  
**Track**: Data Observability (Paradox #T-02)

## Overview

DQ AutoFix is a repair-suggester service that:
- Analyzes failed DQ checks from OpenMetadata
- Detects data quality patterns (nulls, whitespace, duplicates, case issues)
- Proposes automated fixes with confidence scores (0-100%)
- Provides before/after preview with unified diff format
- Generates copy-paste ready SQL with rollback guards

## Quick Start

### Prerequisites

- Python 3.14+
- [uv](https://docs.astral.sh/uv/) package manager
- Docker & Docker Compose (for OpenMetadata)

### 1. Start OpenMetadata

```bash
make docker-up
```

Wait for services to start (2-5 minutes). OpenMetadata will be available at http://localhost:8585

**Default credentials:**
- Username: `admin@open-metadata.org`
- Password: `admin`

### 2. Get JWT Token

1. Open http://localhost:8585
2. Login with admin credentials
3. Go to **Settings** → **Bots** → **ingestion-bot**
4. Click **Copy Token**

### 3. Configure Environment

```bash
# Install dependencies
make install

# Create .env file from template
make env
```

Edit `.env` and add your token:
```bash
OPENMETADATA_HOST=http://localhost:8585
OPENMETADATA_TOKEN=<your-jwt-token>
LOG_LEVEL=INFO
```

### 4. Run the Service

```bash
make dev
```

- API: http://localhost:8000
- Interactive docs: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### 5. Run Tests

```bash
make test

# With coverage
make test-cov
```

## Makefile Commands

Run `make help` to see all available commands:

| Command | Description |
|---------|-------------|
| `make install` | Install dependencies |
| `make dev` | Run development server with auto-reload |
| `make run` | Run production server |
| `make test` | Run tests |
| `make test-cov` | Run tests with coverage |
| `make lint` | Run linter |
| `make format` | Format code |
| `make fix` | Auto-fix linting issues |
| `make typecheck` | Type check src/ |
| `make typecheck-all` | Type check src/ and tests/ |
| `make docker-up` | Start OpenMetadata stack |
| `make docker-down` | Stop OpenMetadata stack |
| `make docker-logs` | View server logs |
| `make docker-ps` | Show running containers |
| `make docker-clean` | Full cleanup with volumes |
| `make clean` | Clean cache files |
| `make check` | Check OpenMetadata API |
| `make api-check` | Check DQ AutoFix API |

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/api/v1/health` | API health with version info |
| GET | `/api/v1/failures` | List failed DQ tests |
| GET | `/api/v1/failures/{id}` | Get specific failure details |
| POST | `/api/v1/analyze` | Analyze failure and get fix recommendations |
| POST | `/api/v1/suggest` | Get best fix suggestion with SQL and preview |
| POST | `/api/v1/preview` | Preview a specific strategy |
| GET | `/api/v1/strategies` | List available fix strategies |

### Example: Analyze a Failure

```bash
curl -X POST http://localhost:8000/api/v1/analyze \
  -H "Content-Type: application/json" \
  -d '{"testCaseId": "your-test-case-id"}'
```

### Example: Get Fix Suggestion

```bash
curl -X POST http://localhost:8000/api/v1/suggest \
  -H "Content-Type: application/json" \
  -d '{"failureId": "your-test-case-id"}'
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENMETADATA_HOST` | `http://localhost:8585` | OpenMetadata server URL |
| `OPENMETADATA_TOKEN` | - | JWT token for authentication (required) |
| `LOG_LEVEL` | `INFO` | Logging level |

## Project Structure

```
openmetadata-dq-autofix/
├── src/dq_autofix/
│   ├── __init__.py              # Package version
│   ├── main.py                  # FastAPI application
│   ├── config.py                # Pydantic Settings
│   ├── api/
│   │   ├── routes.py            # API endpoints
│   │   └── schemas.py           # Request/Response models
│   ├── analyzer/
│   │   ├── failure_analyzer.py  # Main analysis orchestrator
│   │   ├── pattern_detector.py  # Data pattern detection
│   │   └── sample_fetcher.py    # Sample data fetching
│   ├── strategies/
│   │   ├── base.py              # FixStrategy base class
│   │   ├── registry.py          # Strategy registry
│   │   ├── null_imputation.py   # Mean, median, mode, forward fill
│   │   ├── normalization.py     # Trim, case normalization
│   │   └── deduplication.py     # Keep first/last
│   ├── confidence/
│   │   └── scorer.py            # Confidence scoring with patterns
│   ├── preview/
│   │   ├── diff_generator.py    # Before/after diff utilities
│   │   ├── sql_generator.py     # SQL building utilities
│   │   └── rollback.py          # Backup/restore SQL
│   └── openmetadata/
│       ├── client.py            # OpenMetadata API client
│       └── models.py            # Data models
├── tests/
│   ├── test_analyzer/           # Analyzer tests
│   ├── test_strategies/         # Strategy tests
│   ├── test_confidence/         # Confidence scorer tests
│   ├── test_preview/            # Preview utilities tests
│   ├── test_api.py              # API tests
│   └── test_client.py           # Client tests
├── docker-compose.yml           # OpenMetadata stack
├── Makefile                     # Development commands
├── pyproject.toml               # Project configuration
└── README.md
```

## Development

### Code Quality

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
# Check for issues
make lint

# Auto-fix issues
make fix

# Format code
make format
```

### Editor Setup

The project includes configuration for:
- **Zed**: `.zed/settings.json` with ruff and pyright
- **EditorConfig**: `.editorconfig` for consistent styling

Format on save is enabled by default.

### Running Tests

```bash
# Run all tests
make test

# Run with verbose output
uv run pytest -v

# Run with coverage report
make test-cov

# Run specific test file
uv run pytest tests/test_api.py -v
```

## Docker Commands

```bash
# Start OpenMetadata
make docker-up

# Check status
make docker-ps

# View logs
make docker-logs

# Stop services
make docker-down

# Full cleanup (removes data)
make docker-clean
```

## Fix Strategies

| Strategy | Test Type | Reversibility | Description |
|----------|-----------|---------------|-------------|
| Mean Imputation | `columnValuesToNotBeNull` | 50% | Replace nulls with column mean |
| Median Imputation | `columnValuesToNotBeNull` | 50% | Replace nulls with column median |
| Mode Imputation | `columnValuesToNotBeNull` | 60% | Replace nulls with most frequent value |
| Forward Fill | `columnValuesToNotBeNull` | 70% | Fill nulls with previous non-null value |
| Trim Whitespace | `columnValuesToMatchRegex` | 100% | Remove leading/trailing spaces |
| Normalize Case | `columnValuesToBeInSet` | 90% | Convert to lower/upper/title case |
| Keep First | `columnValuesToBeUnique` | 0% | Remove duplicates, keep first occurrence |
| Keep Last | `columnValuesToBeUnique` | 0% | Remove duplicates, keep last occurrence |

### Confidence Scoring

Each fix recommendation includes a confidence score (0-100%) based on:
- **Data Coverage** (25%): How much data we can analyze
- **Pattern Clarity** (25%): How clear the failure pattern is
- **Reversibility** (20%): Can the fix be undone?
- **Impact Scope** (15%): Percentage of rows affected
- **Type Match** (15%): Does the strategy match the data type?

Thresholds:
- **High** (≥80%): Auto-suggest with green indicator
- **Medium** (60-80%): Suggest with warning
- **Low** (40-60%): Flag for review
- **Skip** (<40%): Don't suggest

## Tech Stack

- **Backend**: FastAPI, Pydantic v2, httpx
- **Package Manager**: uv
- **Testing**: pytest, pytest-asyncio, pytest-cov (223 tests)
- **Linting**: Ruff
- **Type Checking**: mypy (strict mode)
- **Infrastructure**: Docker Compose, OpenMetadata 1.6.2

## Links

- [OpenMetadata](https://open-metadata.org/)
- [OpenMetadata Docs](https://docs.open-metadata.org/)
- [Issue #26661](https://github.com/open-metadata/OpenMetadata/issues/26661)
- [WeMakeDevs Hackathon](https://wemakedevs.org/)

## License

MIT
