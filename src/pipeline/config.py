"""Spark session factory with Delta Lake enabled."""
import os
import sys

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Spark launches separate Python worker processes. Pin them to THIS interpreter
# (the venv's Python) so they aren't started under some other `python` on PATH
# that lacks pyspark — which shows up as the executor dying with
# "java.net.SocketException: Connection reset".
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def get_spark(app_name: str = "synaptiq-e1") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")  # local runs: avoid 200 tiny partitions
        .config("spark.sql.session.timeZone", "UTC")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()