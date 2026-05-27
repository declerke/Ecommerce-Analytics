import os
import zipfile
import duckdb
from pathlib import Path
import requests

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/ecommerce.duckdb")
DATA_DIR = Path("/opt/airflow/data/raw")
KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY = os.getenv("KAGGLE_KEY")

DATASET_URL = "https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce"

TABLE_MAP = {
    "olist_orders_dataset.csv": "raw_orders",
    "olist_order_items_dataset.csv": "raw_order_items",
    "olist_order_payments_dataset.csv": "raw_order_payments",
    "olist_order_reviews_dataset.csv": "raw_order_reviews",
    "olist_customers_dataset.csv": "raw_customers",
    "olist_sellers_dataset.csv": "raw_sellers",
    "olist_products_dataset.csv": "raw_products",
    "product_category_name_translation.csv": "raw_product_category_translation",
}


def download_dataset() -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = DATA_DIR / "olist.zip"
    resp = requests.get(
        DATASET_URL,
        auth=(KAGGLE_USERNAME, KAGGLE_KEY),
        stream=True,
        timeout=300,
    )
    resp.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    return zip_path


def extract_zip(zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(DATA_DIR)


def load_to_duckdb() -> None:
    with duckdb.connect(DUCKDB_PATH) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        for csv_file, table_name in TABLE_MAP.items():
            csv_path = DATA_DIR / csv_file
            if not csv_path.exists():
                raise FileNotFoundError(f"Expected CSV not found: {csv_path}")
            con.execute(f"DROP TABLE IF EXISTS raw.{table_name}")
            con.execute(f"""
                CREATE TABLE raw.{table_name} AS
                SELECT * FROM read_csv_auto('{csv_path}', header=True)
            """)
            count = con.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()[0]
            print(f"Loaded raw.{table_name}: {count:,} rows")
