# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

This is a modular data engineering pipeline for scraping and analyzing real estate listings in Dublin. The pipeline follows the Medallion Architecture (raw → bronze → silver → gold) using modern tools for web scraping, data processing, transformation, and orchestration.

## Architecture Components

- **Scraping Layer**: Selenium + Beautiful Soup for web scraping real estate data
- **Storage Layer**: MinIO (S3-compatible) as object storage gateway, Delta Lake via delta-rs for ACID transactions
- **Processing Layer**: Polars for data processing, delta-rs for upserts
- **Transformation Layer**: dbt-core for SQL-based transformations and testing
- **Warehouse Layer**: DuckDB for analytics and querying
- **Orchestration Layer**: Dagster for workflow management
- **Visualization**: Apache Superset for dashboards (future scope)

## Essential Commands

### Environment Setup
```bash
# Create virtual environment
uv venv
source .venv/bin/activate

# Install dependencies
uv pip install -r requirements.txt
```

### MinIO Prerequisites
```bash
# Set MinIO credentials
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT_URL=http://localhost:9000
```

### Running the Full Pipeline
```bash
# Run entire pipeline (scrape → upload → delta lake processing)
make run_all

# Run individual components
make run_prop_scrape  # Web scraping only
make run_delta        # Delta lake processing only
```

### DuckDB Database Setup
```bash
# Initialize DuckDB with Delta Lake views
make duckdb_bootstrap
```

### dbt Commands
```bash
cd dbt/real_estate

# Run all models
dbt run

# Build models with tests
dbt build

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Dagster Development
```bash
# Start Dagster web UI for development
dagster dev

# Run specific assets
dagster asset materialize --select raw_properties
dagster asset materialize --select real_estate_dbt_assets
```

## Key File Locations

- **Pipeline Scripts**: `pipelines/` - Core processing scripts (scraping, MinIO upload, Delta Lake)
- **Dagster Assets**: `dagster_realestate/assets/` - Orchestration definitions
- **dbt Models**: `dbt/real_estate/models/` - SQL transformations organized by layer
- **Helper Functions**: `pipelines/helper_functions.py` - Data processing utilities
- **SQL Bootstrap**: `sql/bootstrap_duckdb.sql` - Database initialization

## Data Flow Architecture

**Lakehouse Architecture (Delta-first approach):**

1. **Staging Layer**: Web scraping → MinIO object storage (NDJSON files by date)
2. **Raw Layer**: Single append-only Delta table (`s3a://delta/delta_table/raw/properties`)
   - Incremental loads from staging with deduplication by `property_id`
3. **Silver Layer**: Watermark-based incremental processing from raw → multiple Delta tables
   - Idempotent upserts using natural/synthetic keys
   - Tracks checkpoint at `s3a://delta/delta_table/_checkpoints/silver_watermark`
4. **Gold Layer**: dbt-core reads Delta silver, writes Delta gold marts
5. **Visualization**: Superset/BI tools query Delta gold tables directly (DuckDB optional for ad-hoc queries)

## Development Environment

- **Python Version**: 3.11+ required
- **Package Manager**: uv (ultrafast Python dependency manager)
- **Configuration**: pyproject.toml with Dagster module configuration
- **Virtual Environment**: `.venv/` directory

## Data Sources and Destinations

- **Source**: Real estate website scraping with change data capture (CDC)
- **Object Storage**: MinIO buckets (`staging`, `delta`)
- **Delta Tables**: S3-compatible paths for versioned data storage
- **Warehouse**: DuckDB file at `data/warehouse.duckdb`
- **dbt Profiles**: Local DuckDB connection with Delta extension

## Testing and Quality

- **dbt Tests**: Data quality tests in `schema.yml` files
- **Unique Constraints**: Property IDs and composite keys
- **Data Expectations**: Using dbt-expectations package
- **Schema Evolution**: Delta Lake handles schema changes automatically

## Important Notes

- **MinIO server must be running** before pipeline execution
- **Raw layer is append-only**: No merges/upserts at raw; deduplication happens at ingestion
- **Silver uses watermark checkpointing**: Only processes `ingested_at > last_watermark` from raw
- **Idempotency**: Re-running `make run_all` won't duplicate data (staging → raw deduplication + silver upserts)
- **dbt models**: Read from Delta silver, write to Delta gold (no DuckDB dependency for transformations)
- **DuckDB role**: Optional for ad-hoc queries via `make duckdb_bootstrap` to create Delta views
- **Dagster workspace** configured in `workspace.yaml`
- **Environment variables required**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL` for MinIO
