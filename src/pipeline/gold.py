"""Gold layer: the analytics mart — per day x category x region.

Rebuilt in full from silver each run. Silver is the source of truth (one row
per order_id, restatements already applied), so a full rebuild is always
correct and trivially idempotent at this volume. At scale you'd recompute only
the affected order_date partitions (driven by what changed in silver).

Policy :
  - returns (negative qty) net out of revenue and units
  - orphan products (not in the dim) are kept under category 'Unknown'
  - rows with no unit_price can't be valued -> excluded (and counted)
  - zero-qty orders are kept (0 revenue/units, still count as an order)
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_daily_sales(spark: SparkSession, silver_path: str, dim_path: str) -> DataFrame:
    orders = spark.read.format("delta").load(silver_path)
    dim = spark.read.format("delta").load(dim_path)

    priced = orders.where(F.col("unit_price").isNotNull())        # (3) exclude unvaluable rows

    joined = (
        priced.join(dim.select("product_id", "category"), "product_id", "left")
        .withColumn("category", F.coalesce("category", F.lit("Unknown")))  # (2) orphans -> Unknown
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))    # (1) qty<0 nets out
    )

    return (
        joined.groupBy("order_date", "category", "region")
        .agg(
            F.sum("revenue").alias("net_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_sold"),                # net units (returns included)
        )
        .withColumn("aov", (F.col("net_revenue") / F.col("order_count")).cast("decimal(10,2)"))
        .orderBy("order_date", "category", "region")
    )


def write_gold(df: DataFrame, table_path: str) -> None:
    ## Full rebuild from silver — overwrite, partitioned by day (idempotent).
    df.write.format("delta").mode("overwrite").partitionBy("order_date").save(table_path) 