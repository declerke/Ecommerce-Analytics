import pytest


def test_mart_order_performance_row_count(con):
    count = con.execute(
        "SELECT COUNT(*) FROM marts.mart_order_performance"
    ).fetchone()[0]
    assert count > 90_000, f"Expected >90k orders in mart, got {count}"


def test_mart_order_performance_delivery_status_values(con):
    invalid = con.execute("""
        SELECT COUNT(*) FROM marts.mart_order_performance
        WHERE delivery_status NOT IN ('On Time', 'Late', 'Cancelled', 'In Progress', 'Unknown')
    """).fetchone()[0]
    assert invalid == 0, f"Found {invalid} rows with invalid delivery_status"


def test_mart_seller_performance_row_count(con):
    count = con.execute(
        "SELECT COUNT(*) FROM marts.mart_seller_performance"
    ).fetchone()[0]
    assert count > 2_500, f"Expected >2,500 sellers in mart, got {count}"


def test_mart_seller_performance_no_null_revenue(con):
    nulls = con.execute(
        "SELECT COUNT(*) FROM marts.mart_seller_performance WHERE total_revenue IS NULL"
    ).fetchone()[0]
    assert nulls == 0, f"Found {nulls} sellers with null total_revenue"


def test_mart_seller_performance_positive_revenue(con):
    non_positive = con.execute(
        "SELECT COUNT(*) FROM marts.mart_seller_performance WHERE total_revenue <= 0"
    ).fetchone()[0]
    assert non_positive == 0, f"Found {non_positive} sellers with non-positive revenue"


def test_mart_product_performance_row_count(con):
    count = con.execute(
        "SELECT COUNT(*) FROM marts.mart_product_performance"
    ).fetchone()[0]
    assert count >= 60, f"Expected >=60 product categories, got {count}"


def test_mart_product_performance_unique_categories(con):
    dupes = con.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT category) FROM marts.mart_product_performance"
    ).fetchone()[0]
    assert dupes == 0, "Duplicate categories found in mart_product_performance"


def test_mart_monthly_revenue_row_count(con):
    count = con.execute(
        "SELECT COUNT(*) FROM marts.mart_monthly_revenue"
    ).fetchone()[0]
    assert count >= 20, f"Expected >=20 months of data, got {count}"


def test_mart_monthly_revenue_chronological(con):
    rows = con.execute(
        "SELECT order_month FROM marts.mart_monthly_revenue ORDER BY order_month"
    ).fetchall()
    months = [r[0] for r in rows]
    assert months == sorted(months), "mart_monthly_revenue is not in chronological order"


def test_mart_payment_analysis_payment_types(con):
    types = {
        r[0]
        for r in con.execute(
            "SELECT DISTINCT payment_type FROM marts.mart_payment_analysis"
        ).fetchall()
    }
    expected = {"credit_card", "boleto", "voucher", "debit_card"}
    assert expected.issubset(types), f"Missing payment types: {expected - types}"


def test_mart_monthly_revenue_positive_gmv(con):
    non_positive = con.execute(
        "SELECT COUNT(*) FROM marts.mart_monthly_revenue WHERE total_revenue <= 0"
    ).fetchone()[0]
    assert non_positive == 0, f"Found {non_positive} months with non-positive GMV"
