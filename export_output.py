"""Export the gold mart to a single CSV for the submission."""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent / "src"))
from pipeline.config import get_spark

GOLD = "data/lakehouse/gold_daily_sales"
OUT = "output/gold_daily_sales.csv"


def main() -> None:
    spark = get_spark("export-output")
    pdf = (
        spark.read.format("delta").load(GOLD)
        .orderBy("order_date", "category", "region")
        .toPandas()
    )
    pathlib.Path("output").mkdir(exist_ok=True)
    pdf.to_csv(OUT, index=False)
    print(f"wrote {len(pdf)} rows -> {OUT}")
    spark.stop()


if __name__ == "__main__":
    main()
