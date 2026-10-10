"""Run once: split the raw PaySim CSV into one Parquet partition per day.

PaySim has no real dates, only `step` (1 step = 1 hour). We pretend the
simulation starts on 2025-01-01, so step 1-24 is Jan 1, step 25-48 is Jan 2...
"""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_DIR = Path(__file__).resolve().parent.parent
RAW_CSV = PROJECT_DIR / "data" / "raw" / "paysim.csv"
OUT_DIR = PROJECT_DIR / "data" / "transactions"

spark = (
    SparkSession.builder.master("local[4]")
    .appName("prepare_data")
    .config("spark.driver.memory", "4g")  # default is 1g, too little for this write
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv(str(RAW_CSV), header=True, inferSchema=True)

df = df.withColumn(
    "txn_date",
    F.date_add(F.lit("2025-01-01").cast("date"), F.floor((F.col("step") - 1) / 24).cast("int")),
)

# repartition by date so each day ends up as a few files instead of hundreds
df.repartition("txn_date").write.mode("overwrite").partitionBy("txn_date").parquet(str(OUT_DIR))

print(f"Rows written: {spark.read.parquet(str(OUT_DIR)).count():,}")
spark.stop()