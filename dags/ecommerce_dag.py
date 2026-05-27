import os
import subprocess

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="ecommerce_analytics",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ecommerce", "analytics", "olist"],
)
def ecommerce_analytics():

    @task(pool="duckdb_pool")
    def download_dataset() -> str:
        from ingestion.loader import download_dataset as _download
        zip_path = _download()
        return str(zip_path)

    @task(pool="duckdb_pool")
    def extract_zip(zip_path: str) -> None:
        from ingestion.loader import extract_zip as _extract
        from pathlib import Path
        _extract(Path(zip_path))

    @task(pool="duckdb_pool")
    def load_raw() -> None:
        from ingestion.loader import load_to_duckdb
        load_to_duckdb()

    @task(pool="duckdb_pool")
    def dbt_run_staging() -> None:
        result = subprocess.run(
            [
                "dbt", "run",
                "--select", "staging",
                "--profiles-dir", "/opt/airflow/dbt",
                "--project-dir", "/opt/airflow/dbt",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        print(result.stdout)

    @task(pool="duckdb_pool")
    def dbt_run_intermediate() -> None:
        result = subprocess.run(
            [
                "dbt", "run",
                "--select", "intermediate",
                "--profiles-dir", "/opt/airflow/dbt",
                "--project-dir", "/opt/airflow/dbt",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        print(result.stdout)

    @task(pool="duckdb_pool")
    def dbt_run_marts() -> None:
        result = subprocess.run(
            [
                "dbt", "run",
                "--select", "marts",
                "--profiles-dir", "/opt/airflow/dbt",
                "--project-dir", "/opt/airflow/dbt",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        print(result.stdout)

    @task()
    def dbt_test() -> None:
        result = subprocess.run(
            [
                "dbt", "test",
                "--profiles-dir", "/opt/airflow/dbt",
                "--project-dir", "/opt/airflow/dbt",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        print(result.stdout)

    @task()
    def log_summary() -> None:
        import duckdb as _duckdb
        path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/ecommerce.duckdb")
        with _duckdb.connect(path, read_only=True) as con:
            orders = con.execute(
                "SELECT COUNT(*) FROM marts.mart_order_performance"
            ).fetchone()[0]
            revenue = con.execute(
                "SELECT ROUND(SUM(total_revenue), 2) FROM marts.mart_monthly_revenue"
            ).fetchone()[0]
            sellers = con.execute(
                "SELECT COUNT(*) FROM marts.mart_seller_performance"
            ).fetchone()[0]
            categories = con.execute(
                "SELECT COUNT(*) FROM marts.mart_product_performance"
            ).fetchone()[0]
        print(
            f"Pipeline complete: {orders:,} orders | BRL {revenue:,.2f} GMV | "
            f"{sellers:,} sellers | {categories} product categories"
        )

    zip_path = download_dataset()
    extracted = extract_zip(zip_path)
    loaded = load_raw()
    extracted >> loaded

    staging = dbt_run_staging()
    intermediate = dbt_run_intermediate()
    marts = dbt_run_marts()
    tested = dbt_test()
    summary = log_summary()

    loaded >> staging >> intermediate >> marts >> tested >> summary


ecommerce_analytics()
