# HubSpot Migrator Tool

## Overview

The Migrator Tool is a Python-based application designed to facilitate the migration of data between HubSpot portals. It provides a command-line interface for managing and executing data migrations, including the ability to extract, transform, and load data between different HubSpot instances.

## Installation

### Prerequisites
- Python 3.13+
- uv (Python package manager)

### Development Installation

1. Clone the repository and navigate to the project directory:
```bash
git clone <repository-url>
cd migrator
```

2. Create a virtual environment and install dependencies using uv:
```bash
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e ".[test]"
```

### Development Usage

There are two ways to run the tool in development mode:

1. Using the installed package entry point:
```bash
migrator test --object-type contacts
```

2. Running the CLI script directly:
```bash
python cli.py test --object-type contacts
```

### Production Installation

```bash
uv pip install migrator
```

### Production Usage
DO NOT KNOW IF WE WILL ACTUALLY DO THIS

```bash
migrator migrate --object-type contacts
```

## Project Structure

```
migrator/
├── src/
│   └── migrator/
│       ├── cli/          # Command-line interface
│       ├── extract/      # Data extraction modules
│       ├── transform/    # Data transformation modules
│       ├── load/        # Data loading modules
│       └── utils/       # Shared utilities
├── tests/              # Test suite
│   ├── conftest.py
│   ├── test_cli/
│   ├── test_extract/
│   ├── test_load/
│   ├── test_transform/
│   └── test_utils/
├── config/            # Configuration templates
│   └── config-pack/   # Default configuration pack
│       ├── migration_config.json
│       ├── contacts/
│       └── companies/
├── cli.py            # Development entry point
├── pyproject.toml    # Package configuration
└── README.md
```

### Key Components

- **CLI Module**: Entry point for all commands (see `src/migrator/cli/commands.py`)
- **Extract Module**: Handles data extraction from source systems
- **Transform Module**: Manages data transformation rules
- **Load Module**: Handles data loading into target systems
- **Utils**: Shared utilities for logging, configuration, etc.

## Command Line Interface

The application's entry point is through `cli.py`, which uses Click for command-line argument parsing. The main commands are:

### Extract only
```bash
migrator extract --object-type companies
```

### Transform only
```bash
migrator transform --object-type deals
```

### Load only
```bash
migrator load --object-type contacts --dry-run
```


### Available Commands

- `migrate`: Full ETL process
- `extract`: Data extraction only
- `transform`: Data transformation only
- `load`: Data loading only
- `test`: Test command for development

## Testing

The project uses pytest for testing with comprehensive coverage reporting.

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=migrator

# Run specific test file
pytest tests/test_extractors.py
```
### Test Configuration

The project includes:
- Coverage configuration (`.coveragerc`)
- Pytest configuration in `pyproject.toml`
- Automated test reporting
- Integration test markers


## Configuration

### Config Pack Structure
Configuration templates are located in `config/config-pack/`. Each object type has its own configuration file:

```
config/config-pack/
├── migration_config.json
├── contacts/
└── companies/
```


## Environment Variables

Required environment variables:
- `SOURCE_HUBSPOT_API_KEY`: Source HubSpot API key
- `TARGET_HUBSPOT_API_KEY`: Target HubSpot API key

## HubSpot Client

The `HubspotClient` provides a simple interface for interacting with the HubSpot API.

### Basic Usage

```python
from migrator.utils.hubspot_client import HubspotClient

# Initialize the client
client = HubspotClient(api_key="your-api-key")

# Make GET request
contacts = client.get("/crm/v3/objects/contacts", params={"limit": 100})

# Make POST request
new_contact = client.post("/crm/v3/objects/contacts", {
    "properties": {
        "email": "test@example.com",
        "firstname": "Test",
        "lastname": "User"
    }
})

# Make PUT request
updated_contact = client.put("/crm/v3/objects/contacts/123", {
    "properties": {
        "firstname": "Updated"
    }
})

# Make DELETE request
client.delete("/crm/v3/objects/contacts/123")
```

### Error Handling

The client provides specific error types for different scenarios:

```python
from migrator.utils.hubspot_client import (
    HubspotError,
    HubspotClientError,
    HubspotServerError,
    HubspotRateLimitError,
    HubspotDuplicateError
)

try:
    result = client.post("/crm/v3/objects/contacts", data)
except HubspotRateLimitError as e:
    # Handle rate limiting
    retry_after = e.retry_after
    print(f"Rate limit hit. Retry after {retry_after} seconds")
except HubspotDuplicateError as e:
    # Handle duplicate records
    print(f"Duplicate found: {e.duplicate_info}")
except HubspotClientError as e:
    # Handle 4xx errors
    print(f"Client error: {e.status_code} - {e.message}")
except HubspotServerError as e:
    # Handle 5xx errors
    print(f"Server error: {e.status_code} - {e.message}")
except HubspotError as e:
    # Handle any other HubSpot-related errors
    print(f"Error: {str(e)}")
```

### Exception Types

| Exception | Description |
|-----------|-------------|
| `HubspotError` | Base exception for all HubSpot-related errors |
| `HubspotClientError` | 4xx client errors (invalid requests) |
| `HubspotServerError` | 5xx server errors |
| `HubspotRateLimitError` | 429 rate limit exceeded |
| `HubspotDuplicateError` | 409 conflict (duplicate record) |

### Common HTTP Status Codes

| Status Code | Meaning | Exception |
|-------------|---------|-----------|
| 200-299 | Successful request | None |
| 400 | Bad request | `HubspotClientError` |
| 401 | Unauthorized | `HubspotClientError` |
| 403 | Forbidden | `HubspotClientError` |
| 404 | Not found | `HubspotClientError` |
| 409 | Conflict/Duplicate | `HubspotDuplicateError` |
| 429 | Rate limit exceeded | `HubspotRateLimitError` |
| 500-599 | Server error | `HubspotServerError` |

## Docker Usage

### Platform Compatibility

The Docker image is built for multiple platforms:
- Linux (AMD64/x86_64)
- Linux (ARM64/aarch64) - for M1/M2 Macs
- Windows (AMD64/x86_64)

### Local Development

Build and run locally:
```bash
# Build the image
docker build -t migrator .

# Run a migration
docker run \
  -e HUBSPOT_SOURCE_API_KEY=xxx \
  -e HUBSPOT_TARGET_API_KEY=yyy \
  migrator migrate --object-type contacts
```

### Using Docker Compose

1. Create a `.env` file with your HubSpot API keys:
```env
HUBSPOT_SOURCE_API_KEY=xxx
HUBSPOT_TARGET_API_KEY=yyy
```

2. Run with Docker Compose:
```bash
docker-compose run migrator migrate --object-type contacts
```

### Using Pre-built Image

Pull and run the latest version:
```bash
docker pull ghcr.io/Aptitude-8/migrator:latest
docker run \
  -e HUBSPOT_SOURCE_API_KEY=xxx \
  -e HUBSPOT_TARGET_API_KEY=yyy \
  ghcr.io/Aptitude-8/migrator:latest migrate --object-type contacts
```


### Windows-Specific Usage

When running on Windows, use the appropriate syntax:

```powershell
# PowerShell
docker run `
  -e HUBSPOT_SOURCE_API_KEY=xxx `
  -e HUBSPOT_TARGET_API_KEY=yyy `
  ghcr.io/Aptitude-8/migrator:latest migrate --object-type contacts

# Command Prompt
docker run ^
  -e HUBSPOT_SOURCE_API_KEY=xxx ^
  -e HUBSPOT_TARGET_API_KEY=yyy ^
  ghcr.io/Aptitude-8/migrator:latest migrate --object-type contacts
```

When using Docker Compose on Windows, ensure line endings in your `.env` file are using Windows-style (CRLF) line endings.

