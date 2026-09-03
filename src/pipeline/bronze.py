""" Bronze ingestion pipeline module. """

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

orders_bronze_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("region", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

def ingest_orders(spark: SparkSession, input_path: str, batch_date: str) -> DataFrame:
    """Ingests one daily orders CSV as raw strings + lineage metadata.

    Args:
        spark (SparkSession): The Spark session.
        input_path (str): Path to the input CSV file (one daily drop).
        batch_date (str): Logical date of the drop being processed, e.g. "2024-01-01".

    Returns:
        DataFrame: The raw ingested rows with lineage columns.
    """
    raw = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(orders_bronze_schema)
        .csv(input_path)
    )

    return (
        raw.withColumn("_source_file", F.input_file_name())
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_batch_date", F.lit(batch_date))   # the drop's date, not wall-clock
    )

def write_bronze(df: DataFrame, table_path: str) -> None:
    """Writes the DataFrame to a Delta Lake table.

    Args:
        df (DataFrame): The DataFrame to write.
        table_path (str): The path to the Delta Lake table.
    """
    (
        df.write
        .format("delta")
        .mode("append")
        .save(table_path)
    )

