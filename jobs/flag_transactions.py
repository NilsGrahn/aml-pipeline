"""Flag suspicious transactions for ONE day.

Usage: python jobs/flag_transactions.py --date 2025-01-05

Reads   data/transactions/txn_date=<date>/
Writes  data/flagged/txn_date=<date>/        (the flagged rows, Parquet)
        data/summary/<date>.json             (counts for the day)

Rerunning the same date overwrites that date's output, so the job is idempotent.
"""
import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data"

# Thresholds. Volumes differ a lot between days in this dataset, so both
# limits are taken from the day's own distribution instead of fixed numbers.
LARGE_TRANSFER_PERCENTILE = 0.99   # top 1% of the day's transfers by amount
FUNNEL_PERCENTILE = 0.999          # top 0.1% of accounts by incoming payments
FUNNEL_MIN_INCOMING = 5            # ...but never fewer than this many payments
FUNNEL_MIN_TOTAL = 500_000         # ...adding up to at least this much

parser = argparse.ArgumentParser()
parser.add_argument("--date", required=True, help="YYYY-MM-DD")
run_date = parser.parse_args().date

spark = (
    SparkSession.builder.master("local[4]")
    .appName(f"flag_{run_date}")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

txns = spark.read.parquet(str(DATA_DIR / "transactions" / f"txn_date={run_date}"))

# Only money leaving an account is interesting here
moves_money = F.col("type").isin("TRANSFER", "CASH_OUT")

# Window: all outgoing payments that land on the same destination account today
per_dest = Window.partitionBy("nameDest")
txns = (
    txns
    .withColumn("dest_incoming_count", F.count(F.when(moves_money, 1)).over(per_dest))
    .withColumn("dest_incoming_total", F.sum(F.when(moves_money, F.col("amount"))).over(per_dest))
    .cache()
)


def percentile(df, column, p):
    values = df.approxQuantile(column, [p], 0.0001)
    return values[0] if values else float("inf")  # no rows: flag nothing


# Today's limits, measured on today's data
large_transfer_limit = percentile(
    txns.filter(F.col("type") == "TRANSFER"), "amount", LARGE_TRANSFER_PERCENTILE
)
accounts = txns.filter(moves_money).select("nameDest", "dest_incoming_count").distinct()
funnel_limit = max(
    FUNNEL_MIN_INCOMING, percentile(accounts, "dest_incoming_count", FUNNEL_PERCENTILE)
)
print(
    f"Limits for {run_date}: large transfer >= {large_transfer_limit:,.0f}, "
    f"funnel account >= {funnel_limit:.0f} incoming payments"
)

flagged = (
    txns
    # Rule 1: unusually large transfer compared with the day's other transfers
    .withColumn(
        "rule_large_transfer",
        (F.col("type") == "TRANSFER") & (F.col("amount") >= large_transfer_limit),
    )
    # Rule 2: the whole balance is moved out in one transaction
    .withColumn(
        "rule_account_drained",
        moves_money
        & (F.col("oldbalanceOrg") > 0)
        & (F.col("newbalanceOrig") == 0)
        & (F.col("amount") == F.col("oldbalanceOrg")),
    )
    # Rule 3: funnel account, far more incoming payments than other accounts today
    .withColumn(
        "rule_funnel_account",
        moves_money
        & (F.col("dest_incoming_count") >= funnel_limit)
        & (F.col("dest_incoming_total") >= FUNNEL_MIN_TOTAL),
    )
    .withColumn(
        "is_flagged",
        F.col("rule_large_transfer") | F.col("rule_account_drained") | F.col("rule_funnel_account"),
    )
)

# One pass over the day's data for all the counts
row = flagged.agg(
    F.count("*").alias("transactions"),
    F.sum(F.col("is_flagged").cast("int")).alias("flagged"),
    F.sum(F.col("rule_large_transfer").cast("int")).alias("rule_large_transfer"),
    F.sum(F.col("rule_account_drained").cast("int")).alias("rule_account_drained"),
    F.sum(F.col("rule_funnel_account").cast("int")).alias("rule_funnel_account"),
    F.sum("isFraud").alias("known_fraud"),
    F.sum(F.when(F.col("is_flagged"), F.col("isFraud")).otherwise(0)).alias("known_fraud_flagged"),
).first()

summary = {"date": run_date, **{k: int(v or 0) for k, v in row.asDict().items()}}

(
    flagged.filter("is_flagged")
    .coalesce(1)  # a few thousand rows, one file per day is enough
    .write.mode("overwrite")
    .parquet(str(DATA_DIR / "flagged" / f"txn_date={run_date}"))
)

summary_dir = DATA_DIR / "summary"
summary_dir.mkdir(parents=True, exist_ok=True)
(summary_dir / f"{run_date}.json").write_text(json.dumps(summary, indent=2))

print(json.dumps(summary, indent=2))
spark.stop()