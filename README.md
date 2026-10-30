# Production-Style Incremental Batch Data Pipeline with PySpark & Delta Lake

A hands-on batch data engineering project demonstrating incremental ingestion, Medallion-style processing, data quality, idempotency, Delta Lake MERGE, and incremental aggregation using PySpark.

## Architecture

```text
CSV Source Files
      |
      v
File Control Framework
      |
      v
BRONZE - Parquet
Raw / Append Only
      |
      v
SILVER PROCESSING
Validation -> Quarantine Invalid Records
      |
      v
Deduplication -> Business Transformations
      |
      v
SILVER - Delta Lake
Delta MERGE / Idempotency
      |
      v
Incremental Gold Processing
Only affected accounts recalculated
      |
      v
GOLD - Delta Lake
Account Summary
```

## Tech Stack

- Python 3.12
- PySpark
- Delta Lake
- PyYAML
- Pytest
- uv
- Git

## Key Features

- Explicit Spark schema instead of schema inference
- Incremental file ingestion using a metadata/control framework
- Raw append-only Bronze layer using Parquet
- Batch-level incremental Silver processing
- Data-quality validation and quarantine handling
- Transaction-level deduplication
- Business transformation using signed transaction amounts
- Delta Lake Silver and Gold tables
- Delta MERGE for idempotent processing
- Incremental Gold processing for affected accounts only
- Pytest unit tests for transformation logic
- Structured application logging
- Spark UI inspection of jobs, stages, tasks, and executors
- Configuration-driven paths using YAML

## Data Quality

Valid transactions require:

- Transaction ID
- Account ID
- Positive transaction amount
- Valid event timestamp
- Transaction type of `DEPOSIT` or `WITHDRAWAL`

Invalid transactions are routed to the quarantine layer.

## Incremental Processing

The pipeline avoids unnecessarily reprocessing all historical data.

**Bronze:** Only previously unprocessed source files are ingested.

**Silver:** Only new Bronze batches are processed.

**Gold:** Only accounts affected by new Silver batches are recalculated before being merged into the Gold Delta table.

## Idempotency

Silver uses `transaction_id` as the business key.

Delta Lake MERGE ensures that retrying or reprocessing a transaction already present in Silver does not create another copy.

The complete pipeline can therefore be rerun safely when there is no new input data.

## Gold Output

The Gold layer maintains account-level metrics:

- Total deposits
- Total withdrawals
- Net amount
- Transaction count

## Run the Project

Install dependencies:

```bash
uv pip install -r requirements.txt
```

Run the pipeline:

```bash
python -m src.jobs.bronze_transactions_job
python -m src.jobs.silver_transactions_job
python -m src.jobs.gold_account_summary_job
```

Run tests:

```bash
python -m pytest -v
```

## Project Structure

```text
config/                 Application configuration
data/source/            Sample input files
docs/                   Project documentation
src/common/             Shared utilities
src/ingestion/          Source ingestion
src/jobs/               Pipeline jobs
src/transformations/    Transformation logic
tests/                  Pytest tests
```

## Learning Outcomes

This project demonstrates the practical relationship between:

**Incremental ingestion → data quality → deduplication → CDC/idempotency → Delta MERGE → incremental aggregation**

It intentionally focuses on batch processing. Kafka and Spark Structured Streaming are handled separately in the next project.