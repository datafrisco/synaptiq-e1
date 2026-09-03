# Notes — Daily Sales Pipeline (Exercise 1)

Short writeup of how I built this, the calls I made, and what I'd change with more time.

## Running it
- Python 3.11 venv, `pip install -r requirements.txt`. Needs Java 17 for Spark, and on Windows a winutils shim (more on that below).
- Run one drop at a time: `python run.py --date 2024-01-01`. Each run goes bronze -> silver -> dim -> gold for that day.
- Everything lands under `data/lakehouse/` as Delta tables. The final mart is `gold_daily_sales`.

## How it's built
Medallion layout (bronze/silver/gold). The job runs daily, so I wanted each stage to be re-runnable and easy to reason about on its own.

- **Bronze** (`bronze.py`): lands the raw CSV exactly as it arrived. Everything as strings, bad rows kept in `_corrupt_record`, plus lineage columns (source file, ingest time, batch date). No cleaning here on purpose, so I can always replay from raw.
- **Silver** (`silver.py`): cleaning and typing, and where idempotency lives. Dedupes to one row per `order_id` and MERGEs into the silver table. Re-running a day doesn't create duplicates, and a restated order overwrites the old one.
- **Dim** (`dim_product.py`): the product reference, with categories normalized.
- **Gold** (`gold.py`): the mart. Per day / category / region: net revenue, order count, units sold, AOV.

## Assumptions
- One CSV row is one order line. `order_id` is unique per order (each order has a single product in this data).
- "Net revenue" means sum(quantity * unit_price), with returns (negative quantity) left in so they net out. That felt like the point of the word "net".
- `unit_price` is the actual sale price, not the catalog `list_price`.
- New files keep the same shape and arrive as `orders_YYYY-MM-DD.csv`, one per day.
- Regions and categories that only differ by case are the same value (`west` = `West`).
- I was handed `products.xlsx`; the brief says `products.csv`. I assumed the xlsx is the intended reference and read it as-is.

## Decisions where the brief left room
- **batch_date is the drop's date, not today.** I pass the date in and stamp that instead of the wall-clock date. Otherwise replaying an old file would relabel it and break anything keyed on the batch. On a normal daily schedule they line up anyway.
- **Bronze appends, silver is the idempotency gate.** Bronze keeps every delivery, re-sends included, as a raw log. Silver collapses to the latest per `order_id` and merges. Keeps the raw history without letting it pollute the numbers.
- **Returns net out.** The `-1` quantity row flows through as negative revenue and negative units.
- **Orphan products stay in, labeled `Unknown`.** Two order rows point at products that aren't in the reference (`P099`, `P011`). I left-join and label them `Unknown` rather than drop them, so revenue still reconciles and the gap is visible.
- **No price, no value.** One row has no `unit_price`, so I can't value it. I drop it from the mart and print the count instead of losing it silently or faking a zero.
- **Zero-quantity orders kept.** They add nothing but still count as an order. This does pull AOV down where one sits next to a real order (Stationery/North on 2024-01-01, AOV 10.50). Could go either way. I kept them since they're real orders.
- **Malformed rows kept and flagged.** One row had an extra column. The seven real fields parsed fine, so I kept it and set `_is_malformed = true` instead of throwing away a real order.
- **AOV is net revenue / distinct orders.**
- **Money is decimal, not float.** Don't want float rounding on revenue.
- **Everything runs in UTC.** I pinned the Spark session to UTC so dates are deterministic. This caught a real bug: one date was a Unix epoch, and in local time it landed on the wrong day.

## What was messy in the data
Most of the work was here. Across the two files:
- Prices formatted a few ways: `$24.99`, `"$1,099.00"` (comma and quotes), plain `3.50`.
- Three date formats: ISO, US `MM/DD/YYYY`, and a Unix epoch.
- Region casing all over the place (`west`, `East`, `east`).
- Category casing in the products file too (`Electronics` vs `electronics`).
- Blank `customer_id` on a couple of rows, and one row missing both price and region.
- Quantities of 0 and -1.
- Two products not in the reference.
- A row with an extra trailing column.
- Day 2 re-sent two day-1 orders, one identical and one with a changed quantity. That's what drove the dedupe and MERGE.

## Local vs Databricks / Delta
Built on local PySpark + delta-spark, so the Delta and MERGE code ports straight over. What I'd do differently on the real platform:
- **Ingestion:** Auto Loader (`cloudFiles`) instead of passing a date and picking the file myself. It handles new-file discovery and checkpointing.
- **MERGE:** same API, no change. delta-spark locally is the same as Delta on a cluster.
- **The Windows setup pain isn't a thing there.** Locally I had to install winutils/HADOOP_HOME and pin `PYSPARK_PYTHON` to the venv. None of that applies on a Linux cluster.
- **Products reference:** read with pandas locally since Spark can't read xlsx. On Databricks I'd land it as a managed Delta table, and probably SCD2 if history matters.
- **Data quality:** the `_is_malformed` and missing-price handling would become DLT expectations or table constraints, with proper quarantine tables.
- **Scheduling:** a Databricks Workflow instead of running `run.py` by hand.

## With more time
- Real **quarantine tables** for rejects (missing price, malformed) instead of a filter and a printed count.
- **Unit tests** on the cleaning functions (currency, the epoch date, dedupe, the merge) and a small end-to-end run. That's the logic most likely to break silently.
- **Incremental gold** instead of a full rebuild. At volume you'd recompute only the partitions that changed. One catch: a restated day-1 order can show up in a day-2 file, so you recompute the order_dates that actually changed, not just "today".
- Chase down the **orphan products**: real products missing from the reference, or bad IDs? Question for whoever owns the source.
- Swap the `print`s for real logging, move paths into config, add row-count and freshness checks.

