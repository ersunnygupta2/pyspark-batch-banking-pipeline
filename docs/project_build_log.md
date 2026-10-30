# Project Build Log

## Project Goal

Build a production-style incremental banking transaction pipeline locally using PySpark and Delta Lake while understanding each implementation decision rather than relying on prebuilt code.

## 1. Local Spark Environment

Configured:

- Python 3.12
- Java 17
- PySpark
- Delta Lake
- uv virtual environment

Created a reusable SparkSession utility so individual jobs do not duplicate Spark initialization code.

## 2. Explicit Transaction Schema

Defined a `StructType` schema for banking transactions instead of relying on schema inference.

Fields include:

- transaction_id
- account_id
- transaction_type
- amount
- event_timestamp

## 3. Configuration Management

Created `application.yaml` to separate paths and configuration from application code.

PyYAML loads the configuration into Python dictionaries.

## 4. Incremental Bronze Ingestion

Bronze stores raw transactions in Parquet format.

Each ingested record receives operational metadata including:

- batch_id
- ingestion_timestamp
- source_file

A file-control framework records previously processed source files.

The Bronze job therefore processes only files that have not already been successfully ingested.

The source file is marked as processed only after the Bronze write succeeds.

## 5. Incremental Silver Processing

Silver maintains a separate control file containing Bronze batch IDs that have already been processed.

The Silver job identifies only new Bronze batches instead of intentionally processing all historical batches again.

## 6. Data Quality and Quarantine

Incoming transactions are separated into valid and invalid records.

Validation checks include:

- Required transaction ID
- Required account ID
- Positive amount
- Supported transaction type
- Valid event timestamp

Invalid transactions are written to the quarantine layer for investigation rather than silently discarded.

## 7. Deduplication

Valid incoming records are deduplicated using `transaction_id`.

This handles duplicate business events appearing within incoming incremental data.

Deduplication and idempotency were treated as separate concepts.

## 8. Business Transformation

Created `signed_amount`:

- DEPOSIT -> positive amount
- WITHDRAWAL -> negative amount

This simplifies downstream account balance aggregation.

## 9. Delta Lake Silver

Silver was implemented as a Delta Lake table.

This introduced:

- Parquet data files
- `_delta_log`
- ACID transaction support
- Delta MERGE

For immutable transaction events, the MERGE inserts transactions that do not already exist.

Existing `transaction_id` values are not inserted again.

This provides target-level idempotency.

## 10. Incremental Gold Processing

Gold was deliberately implemented incrementally rather than recalculating every account after every batch.

The process is:

1. Identify new Silver batches.
2. Determine accounts affected by those batches.
3. Retrieve complete Silver history only for those accounts.
4. Recalculate their account summaries.
5. MERGE those account rows into Gold.

Gold metrics include:

- total_deposits
- total_withdrawals
- net_amount
- transaction_count

## 11. Incremental Processing Test

Additional batches were introduced to verify the design.

A duplicate transaction demonstrated Silver MERGE idempotency.

A new transaction affecting only one account demonstrated that incremental Gold recalculated only the affected account rather than every account.

## 12. Pytest

Added unit tests for:

- Valid/invalid transaction handling
- Deduplication
- Signed amount transformation

A session-scoped pytest fixture creates one SparkSession for the test session and stops it after all tests finish.

Tests are executed using:

```bash
python -m pytest -v
```

## 13. Logging

Replaced important `print()` statements with Python logging.

Logs identify:

- Timestamp
- Log level
- Python module
- Message

This provides more structured operational visibility than console print statements.

## 14. Idempotency Test

The Bronze, Silver, and Gold jobs were rerun without introducing new input data.

Results:

- Bronze detected no new files.
- Silver detected no new Bronze batches.
- Gold detected no new Silver batches.

No duplicate business records were produced.

## 15. Spark UI

Temporarily kept a Spark application alive and inspected the local Spark UI.

Reviewed the relationship:

```text
Spark Application
      |
    Action
      |
     Job
      |
   Stages
      |
    Tasks
      |
Partitions processed by executors
```

Also inspected shuffle and executor information.

## Final Architecture

```text
CSV
 |
 v
File Control
 |
 v
Bronze (Parquet / Raw / Append)
 |
 v
Validation
 |------------------> Quarantine (Invalid)
 |
 v
Deduplication + Transformation
 |
 v
Silver (Delta)
 |
 v
Delta MERGE / Idempotency
 |
 v
Affected Account Detection
 |
 v
Incremental Aggregation
 |
 v
Gold (Delta)
```

## Important Design Lessons

- Bronze raw data and downstream business data serve different purposes.
- Incremental ingestion and CDC are related but not identical concepts.
- Deduplication and idempotency solve different problems.
- Delta MERGE provides a reliable mechanism for maintaining target state.
- Incremental Gold processing does not mean calculating aggregates using only new rows; affected entities may require their historical records to calculate their new complete state.
- Control metadata is an important part of production data pipelines.

## Simplifications

This is a local learning implementation.

The JSON-based control framework represents the same concept that would commonly be implemented using production metadata/control tables.

The project intentionally excludes streaming. Kafka, Spark Structured Streaming, checkpoints, offsets, watermarking, late events, and combined batch/streaming processing will be introduced in Project 2.