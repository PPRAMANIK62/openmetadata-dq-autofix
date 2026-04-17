# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - 2026-04-17

### Added

#### Phase 1: Foundation

**Core Application**
- FastAPI application with health check endpoints
- Pydantic Settings for configuration management
- OpenMetadata API client with async httpx support
- JWT token authentication

**API Endpoints**
- `GET /health` - Root health check
- `GET /api/v1/health` - API health with version info
- `GET /api/v1/failures` - List failed DQ test cases
- `GET /api/v1/failures/{id}` - Get specific failure details

**Data Models**
- `TestCaseResult` - DQ test case with results
- `TestResultStatus` - Test status enum (StrEnum)
- `SampleData` - Table sample data
- `TableProfile` - Column statistics
- `ColumnProfile` - Individual column stats

**Infrastructure**
- Docker Compose for OpenMetadata 1.6.2 stack
  - OpenMetadata server
  - MySQL database
  - Elasticsearch for search
  - Airflow for ingestion workflows
- CORS middleware enabled

**Testing**
- pytest test suite with pytest-asyncio
- Integration tests with real OpenMetadata API
- Automatic test skipping when token not configured
- 11 tests covering API and client functionality

**Developer Experience**
- Makefile with common commands
- Ruff for linting and formatting
- EditorConfig for consistent styling
- Zed editor configuration
- Interactive API docs at `/docs` (Swagger UI)
- ReDoc documentation at `/redoc`

### Configuration

**Environment Variables**
- `OPENMETADATA_HOST` - OpenMetadata server URL
- `OPENMETADATA_TOKEN` - JWT token for authentication
- `LOG_LEVEL` - Logging level (default: INFO)

**Ruff Lint Rules**
- E, W: pycodestyle
- F: pyflakes
- I: isort
- UP: pyupgrade
- B: flake8-bugbear
- SIM: flake8-simplify
- C4: flake8-comprehensions
- DTZ: flake8-datetimez
- RUF: ruff-specific rules

### Project Structure

```
openmetadata-dq-autofix/
├── src/dq_autofix/           # Main application
│   ├── api/                  # FastAPI routes and schemas
│   └── openmetadata/         # OM client and models
├── docker-compose.yml        # OpenMetadata Docker stack
├── tests/                    # Test suite
├── .zed/                     # Zed editor config
├── Makefile                  # Development commands
└── pyproject.toml            # Project configuration
```

### Dependencies

**Runtime**
- fastapi >= 0.136.0
- uvicorn[standard] >= 0.44.0
- pydantic >= 2.13.2
- pydantic-settings >= 2.13.1
- httpx >= 0.28.1
- python-dotenv >= 1.2.2

**Development**
- pytest >= 9.0.3
- pytest-asyncio >= 1.3.0
- pytest-cov >= 7.1.0
- ruff >= 0.15.11
