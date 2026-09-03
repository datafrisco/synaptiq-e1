"""Silver layer: clean, type, and standardize bronze orders."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable


def clean_orders(bronze_df: DataFrame) -> DataFrame:
    return (
        bronze_df
        # mixed formats: ISO, US M/D/Y, and Unix epoch -> one real DATE
        .withColumn("order_date", F.coalesce(
            F.to_date("order_date", "yyyy-MM-dd"),
            F.to_date("order_date", "M/d/yyyy"),
            F.to_date(F.from_unixtime(F.col("order_date").cast("long"))),
        ))
        # cast quantity to int. 
        .withColumn("quantity", F.col("quantity").cast("int"))
        # strip $ and commas; always use DECIMAL(10,2) for money; e.g. $1,234.56 -> 1234.56
        .withColumn("unit_price",
                    F.regexp_replace("unit_price", r"[$,]", "").cast("decimal(10,2)"))
        # west/East -> West/East
        .withColumn("region", F.initcap(F.trim("region")))       
        # Flag records that bronze determined to be malformed (e.g. missing required fields, bad types, etc.). 
        .withColumn("_is_malformed", F.col("_corrupt_record").isNotNull())
        #Drop the full _corrupt_record column, but keep the boolean flag for downstream filtering.
        .drop("_corrupt_record")
    )


def _dedupe_latest(df: DataFrame) -> DataFrame:
    ## Collapse re-delivered rows to one per order_id, keeping the latest ingest.
    w = Window.partitionBy("order_id").orderBy(F.col("_ingest_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def upsert_orders(spark: SparkSession, clean_df: DataFrame, table_path: str) -> None:
    ## Idempotent MERGE of a cleaned batch into silver, keyed on order_id (last write wins).
    src = _dedupe_latest(clean_df)
    if not DeltaTable.isDeltaTable(spark, table_path):
        src.write.format("delta").save(table_path)      # first run creates the table
        return
    (DeltaTable.forPath(spark, table_path).alias("t")
        .merge(src.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()      # restatement wins
        .whenNotMatchedInsertAll()   # new orders inserted
        .execute())