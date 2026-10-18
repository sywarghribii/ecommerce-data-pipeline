import logging
import sys
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from ingestion.spark_session import get_spark

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data" / "raw"
BRONZE = BASE / "data" / "bronze"
(BASE / "logs").mkdir(exist_ok=True)

SOURCES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(BASE / "logs" / "bronze_csv.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("bronze_csv")


def ingest_table(spark, name, filename, run_date):
    src = RAW / filename
    if not src.exists():
        raise FileNotFoundError(f"Fichier source introuvable : {src}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(str(src))
    )

    df = (
        df.withColumn("_ingestion_date", F.lit(run_date))
          .withColumn("_source_file", F.lit(filename))
          .withColumn("_ingested_at", F.current_timestamp())
    )

    rows = df.count()
    (
        df.write
        .mode("overwrite")
        .partitionBy("_ingestion_date")
        .parquet(str(BRONZE / name))
    )
    log.info("OK %-12s | %8d lignes | partition %s", name, rows, run_date)
    return rows


def main(run_date=None):
    run_date = run_date or date.today().isoformat()
    spark = get_spark("bronze_csv")
    try:
        for name, filename in SOURCES.items():
            try:
                ingest_table(spark, name, filename, run_date)
            except Exception:
                log.exception("ECHEC ingestion de %s", name)
                raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
