"""Pipeline entry point — runs the daily pipeline for one order-drop date.

    python run.py --date 2024-01-01
"""
import argparse
import pathlib
import sys
from pyspark.sql import functions as F

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent / "src"))

from pipeline.config import get_spark
from pipeline import bronze
from pipeline import silver
from pipeline import dim_product
from pipeline import gold

LANDING_ORDERS = "data/landing/orders/orders_{date}.csv"
LANDING_PRODUCTS = "data/landing/products/products.xlsx"

BRONZE_ORDERS = "data/lakehouse/bronze_orders"
SILVER_ORDERS = "data/lakehouse/silver_orders"
DIM_PRODUCT = "data/lakehouse/dim_product"
GOLD_DAILY_SALES = "data/lakehouse/gold_daily_sales"
    
def main(date: str) -> None:
    spark = get_spark(f"pipeline-{date}")
    input_path = LANDING_ORDERS.format(date=date)

    print(f"[bronze] ingesting {input_path}")
    raw = bronze.ingest_orders(spark, input_path, batch_date=date)
    bronze.write_bronze(raw, BRONZE_ORDERS)

    print(f"[bronze] {BRONZE_ORDERS} now holds:")
    spark.read.format("delta").load(BRONZE_ORDERS).show(truncate=False)
    
    print("[silver] cleaned view of this batch:")
    bronze_batch = (spark.read.format("delta").load(BRONZE_ORDERS)
                    .where(F.col("_batch_date") == date))

    clean = silver.clean_orders(bronze_batch)
    silver.upsert_orders(spark, clean, SILVER_ORDERS)
    print (f"[silver] {SILVER_ORDERS} now holds:")
    spark.read.format("delta").load(SILVER_ORDERS).orderBy("order_id").show(truncate=False)

    # Product Dim
    print("[dim] building product dimension")
    dim = dim_product.build_product_dim(spark, LANDING_PRODUCTS)
    dim_product.write_dim(dim, DIM_PRODUCT)
    spark.read.format("delta").load(DIM_PRODUCT).orderBy("product_id").show(truncate=False)

    # Gold Layer
    # after the dim block:
    print("[gold] building daily sales mart")
    excluded = (spark.read.format("delta").load(SILVER_ORDERS)
                .where(F.col("unit_price").isNull()).count())
    if excluded:
        print(f"[gold] excluded {excluded} row(s) with no unit_price (cannot be valued)")
    mart = gold.build_daily_sales(spark, SILVER_ORDERS, DIM_PRODUCT)
    gold.write_gold(mart, GOLD_DAILY_SALES)
    spark.read.format("delta").load(GOLD_DAILY_SALES).show(truncate=False)



    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="order-drop date, e.g. 2024-01-01")
    main(ap.parse_args().date)