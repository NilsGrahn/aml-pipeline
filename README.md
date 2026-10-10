# AML Transaction Monitoring Pipeline

A daily batch pipeline that screens bank transactions for suspicious
patterns. Apache Airflow schedules one run per day; each run starts a
PySpark job that flags transactions and writes the result as Parquet.

## Pipeline
check_input -> flag_transactions (PySpark) -> validate_output

[screenshot of the Airflow grid with 31 successful runs]

## Rules
- Large transfer: a single TRANSFER above 200,000
- Account drained: the full balance leaves the account in one transaction
- Funnel account: 5+ incoming payments to one account in a day (window function)

## Results
[paste the output of jobs/report.py here]

## Design choices
- Data is partitioned by date, so each run reads one day only
- Runs are idempotent: rerunning a date overwrites that date's output
- Catchup backfills all 31 days from a single DAG definition

## Run it
[the commands from steps 1, 2 and 5]

## Limitations
Runs locally in Spark local mode on synthetic data (PaySim). The rules
are simple thresholds, not a model. The dataset fits in pandas; Spark is
used here to practise the API.