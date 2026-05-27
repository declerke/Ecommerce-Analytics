import os
import pytest
import duckdb

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/ecommerce.duckdb")


@pytest.fixture(scope="module")
def con():
    with duckdb.connect(DUCKDB_PATH, read_only=True) as c:
        yield c


def test_all_raw_tables_exist(con):
    tables = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
        ).fetchall()
    }
    expected = {
        "raw_orders",
        "raw_order_items",
        "raw_order_payments",
        "raw_order_reviews",
        "raw_customers",
        "raw_sellers",
        "raw_products",
        "raw_product_category_translation",
    }
    assert expected.issubset(tables), f"Missing tables: {expected - tables}"


def test_orders_row_count(con):
    count = con.execute("SELECT COUNT(*) FROM raw.raw_orders").fetchone()[0]
    assert count > 90_000, f"Expected >90k orders, got {count}"


def test_order_items_row_count(con):
    count = con.execute("SELECT COUNT(*) FROM raw.raw_order_items").fetchone()[0]
    assert count > 100_000, f"Expected >100k order items, got {count}"


def test_order_payments_row_count(con):
    count = con.execute("SELECT COUNT(*) FROM raw.raw_order_payments").fetchone()[0]
    assert count > 95_000, f"Expected >95k payments, got {count}"


def test_order_reviews_row_count(con):
    count = con.execute("SELECT COUNT(*) FROM raw.raw_order_reviews").fetchone()[0]
    assert count > 90_000, f"Expected >90k reviews, got {count}"


def test_customers_row_count(con):
    count = con.execute("SELECT COUNT(*) FROM raw.raw_customers").fetchone()[0]
    assert count > 90_000, f"Expected >90k customers, got {count}"


def test_sellers_row_count(con):
    count = con.execute("SELECT COUNT(*) FROM raw.raw_sellers").fetchone()[0]
    assert count > 2_500, f"Expected >2,500 sellers, got {count}"


def test_products_row_count(con):
    count = con.execute("SELECT COUNT(*) FROM raw.raw_products").fetchone()[0]
    assert count > 30_000, f"Expected >30k products, got {count}"


def test_product_categories_row_count(con):
    count = con.execute(
        "SELECT COUNT(*) FROM raw.raw_product_category_translation"
    ).fetchone()[0]
    assert count >= 60, f"Expected >=60 category translations, got {count}"


def test_orders_no_null_order_id(con):
    nulls = con.execute(
        "SELECT COUNT(*) FROM raw.raw_orders WHERE order_id IS NULL"
    ).fetchone()[0]
    assert nulls == 0, f"Found {nulls} null order_ids"


def test_order_items_positive_price(con):
    non_positive = con.execute(
        "SELECT COUNT(*) FROM raw.raw_order_items WHERE CAST(price AS DOUBLE) <= 0"
    ).fetchone()[0]
    assert non_positive == 0, f"Found {non_positive} non-positive prices"
