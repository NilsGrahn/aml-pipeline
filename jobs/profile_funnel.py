"""One-off profiling: how many payments does a destination account receive per day?

Used to pick sensible thresholds for the funnel-account rule.
Usage: python jobs/profile_funnel.py 2025-01-01 2025-01-10
"""
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

spark = (
    SparkSession.builder.master("local[4]")
    .appName("profile_funnel")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

for run_date in sys.argv[1:]:
    txns = spark.read.parquet(str(DATA_DIR / "transactions" / f"txn_date={run_date}"))
    total_rows = txns.count()

    per_dest = (
        txns.filter(F.col("type").isin("TRANSFER", "CASH_OUT"))
        .groupBy("nameDest")
        .agg(F.count("*").alias("n"), F.sum("amount").alias("total"))
        .cache()
    )

    q = per_dest.approxQuantile("n", [0.5, 0.9, 0.99, 0.999, 1.0], 0.0001)
    print(f"\n=== {run_date}: {total_rows:,} transactions, {per_dest.count():,} destination accounts ===")
    print(f"incoming payments per account: median {q[0]:.0f}, p90 {q[1]:.0f}, p99 {q[2]:.0f}, p99.9 {q[3]:.0f}, max {q[4]:.0f}")
    print(f"{'min_incoming':>12} {'min_total':>12} {'accounts':>9} {'flagged_txns':>13} {'share':>7}")
    for min_n in (5, 10, 15, 20, 30, 50):
        for min_total in (500_000, 5_000_000):
            hit = per_dest.filter((F.col("n") >= min_n) & (F.col("total") >= min_total))
            row = hit.agg(F.count("*").alias("a"), F.sum("n").alias("t")).first()
            flagged = row["t"] or 0
            print(f"{min_n:>12} {min_total:>12,} {row['a']:>9,} {flagged:>13,} {flagged / total_rows:>7.2%}")
    per_dest.unpersist()

spark.stop()
