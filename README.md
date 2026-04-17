# OpenMetadata DQ AutoFix

AI-powered repair suggestions for failed OpenMetadata Data Quality checks.

## Overview

DQ AutoFix is a repair-suggester service that:
- Analyzes failed DQ checks from OpenMetadata
- Proposes automated fixes with confidence scores
- Provides safe preview with rollback guards
- Generates copy-paste ready SQL statements

## Quick Start

### Prerequisites

- Python 3.11+
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

### Coming Soon (Phase 2+)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/analyze` | Analyze failures and suggest fixes |
| POST | `/api/v1/preview` | Preview fix with before/after diff |
| GET | `/api/v1/strategies` | List available fix strategies |

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
│   ├── __init__.py          # Package version
│   ├── main.py              # FastAPI application
│   ├── config.py            # Pydantic Settings
│   ├── api/
│   │   ├── routes.py        # API endpoints
│   │   └── schemas.py       # Request/Response models
│   └── openmetadata/
│       ├── client.py        # OpenMetadata API client
│       └── models.py        # Data models
├── docker-compose.yml       # OpenMetadata stack
├── tests/
│   ├── conftest.py          # Pytest fixtures
│   ├── test_api.py          # API tests
│   └── test_client.py       # Client tests
├── .zed/
│   └── settings.json        # Zed editor settings
├── .editorconfig            # Editor configuration
├── .env.example             # Environment template
├── Makefile                 # Development commands
├── pyproject.toml           # Project configuration
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

## Fix Strategies (Planned)

| Strategy | Test Type | Description |
|----------|-----------|-------------|
| Mean Imputation | `columnValuesToNotBeNull` | Replace nulls with column mean |
| Median Imputation | `columnValuesToNotBeNull` | Replace nulls with column median |
| Mode Imputation | `columnValuesToNotBeNull` | Replace nulls with most frequent value |
| Trim Whitespace | `columnValuesToMatchRegex` | Remove leading/trailing spaces |
| Normalize Case | `columnValuesToBeInSet` | Convert to lower/upper/title case |
| Deduplicate | `columnValuesToBeUnique` | Remove duplicate rows |

## Tech Stack

- **Backend**: FastAPI, Pydantic v2, httpx
- **Package Manager**: uv
- **Testing**: pytest, pytest-asyncio, pytest-cov
- **Linting**: Ruff
- **Infrastructure**: Docker Compose, OpenMetadata 1.6.2

## Links

- [OpenMetadata](https://open-metadata.org/)
- [OpenMetadata Docs](https://docs.open-metadata.org/)
- [Issue #26661](https://github.com/open-metadata/OpenMetadata/issues/26661)
- [WeMakeDevs Hackathon](https://wemakedevs.org/)

## License

MIT
