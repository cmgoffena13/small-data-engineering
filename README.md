# Small Data

[![build](https://github.com/cmgoffena13/small-data/actions/workflows/build.yml/badge.svg)](https://github.com/cmgoffena13/small-data/actions/workflows/build.yml)
![linesofcode](https://aschey.tech/tokei/github/cmgoffena13/small-data?category=code)

## Data Engineering Tech Stack
1. Polars - Ingestion / Transformation
2. Delta-rs - Storage
3. Duckdb - OLAP Queries
4. Streamlit - Presentation
5. Dagster - Orchestration
6. UV - Environment Management
7. Docker - Easy Deployment

## Setup
1. Install UV (Check your installation using `uv --version`)  
Mac: `curl -LsSf https://astral.sh/uv/0.6.9/install.sh | sh`  
Windows: `powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/0.6.9/install.ps1 | iex"`
2. Install the necessary project dependencies using `uv sync`
3. Install pre-commit: `uv run pre-commit install --install-hooks`

### Optional - Duckdb UI
1. Install Duckdb CLI  
Mac: `curl https://install.duckdb.org | sh`  
Windows: `winget install DuckDB.cli`
2. Install Duckdb UI: `duckdb -ui`

### Using Duckdb ad-hoc
1. Navigate to the repo directory
2. Use the `duckdb` command
3. Query a delta table in the command line IDE:  
 `SELECT * FROM delta_scan('./dev_data/target/employees') LIMIT 100`

## Development

## Deployment

