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
## Results

All 31 days, 6,362,620 transactions:

| | Fixed thresholds | Per-day percentiles |
| --- | ---: | ---: |
| Transactions flagged | 656,047 (10.31%) | 30,123 (0.47%) |
| Known fraud caught (recall) | 99.6% | 99.2% |
| Flags that are known fraud (precision) | 1.2% | 27.0% |

The first version used fixed limits (transfers above 200,000, accounts
with 5+ incoming payments). The validation task failed the run for
2025-01-01, where 34% of transactions were flagged. Profiling
(jobs/profile_funnel.py) showed why: daily volume ranges from 272 to
574,255 transactions, so a fixed limit is extreme on one day and
ordinary on another. Both limits are now percentiles of each day's own
distribution.

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