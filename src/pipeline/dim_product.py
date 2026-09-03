"""Product dimension — reference data describing each product.

Product is an XLSX file. We'll need to read it via pandas/openpyxl 
"""
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_product_dim(spark: SparkSession, xlsx_path: str) -> DataFrame:
    ## Read products.xlsx and return a deduped product dimension.
    pdf = pd.read_excel(xlsx_path, engine="openpyxl")
    return (
        spark.createDataFrame(pdf)
        .withColumn("category", F.initcap(F.trim("category")))   # electronics -> Electronics
        .dropDuplicates(["product_id"])
    )


def write_dim(df: DataFrame, table_path: str) -> None:
    ## Reference data is a full snapshot — overwrite each run 
    df.write.format("delta").mode("overwrite").save(table_path)