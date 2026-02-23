from datetime import date
from src.transforms.silver import silver_enriched_orders,silver_enriched_customers,silver_enriched_products,silver_enriched_order_details


def test_silver_enriched_orders(orders_df):

    result = silver_enriched_orders(orders_df)
    rows = result.collect()

    
    assert len(rows) == 1
    assert all(r.order_id == "O1" for r in rows)

    
    expected_cols = [
        "order_id", "customer_id", "product_id",
        "price", "profit", "quantity", "discount", "row_id",
        "order_date", "ship_date", "year", "total_amount"
    ]
    assert set(expected_cols).issubset(set(result.columns))

    row = rows[0]

    assert isinstance(row.price, float)
    assert isinstance(row.profit, float)
    assert isinstance(row.quantity, int)
    assert isinstance(row.discount, float)
    assert isinstance(row.row_id, int)
    assert isinstance(row.order_date, date)
    assert isinstance(row.ship_date, date)

    
    assert row.year == 2023
    assert row.total_amount == row.price * row.quantity * (1 - row.discount)

    
    assert row.order_id == "O1"
    assert row.customer_id == "C1"
    assert row.product_id == "P1"
    assert row.total_amount == 180.0



def test_silver_enriched_customers(customers_df):

    result = silver_enriched_customers(customers_df)
    rows = result.collect()

    # No null customer_id
    assert all(r.customer_id is not None for r in rows)

    # Column validation
    expected_cols = [
        "customer_id", "customer_name", "email", "phone",
        "address", "segment", "country", "city",
        "state", "postal_code", "region"
    ]
    assert set(expected_cols).issubset(set(result.columns))

    # Type validation
    for r in rows:
        if r.postal_code is not None:
            assert isinstance(r.postal_code, int)


def test_silver_enriched_products(products_df):

    result = silver_enriched_products(products_df)
    rows = result.collect()

    expected_cols = [
        "product_id", "category", "sub_category",
        "product_name", "state", "price_per_product"
    ]
    assert set(expected_cols).issubset(set(result.columns))

    assert all(r.product_id is not None for r in rows)

    for r in rows:
        assert isinstance(r.price_per_product, float)



def test_silver_enriched_order_details(
    orders_details_df,
    customers_df,
    products_df
):

    result = silver_enriched_order_details(
        orders_details_df,
        customers_df,
        products_df
    )

    rows = result.collect()

    assert len(rows) == 2

    expected_cols = [
        "order_id", "customer_id", "customer_name", "country",
        "product_id", "category", "sub_category",
        "price", "quantity", "discount", "total_amount", "profit",
        "order_date", "ship_date", "year"
    ]
    assert set(expected_cols).issubset(set(result.columns))

    for row in rows:
        # Profit rounded
        assert round(row.profit, 2) == row.profit

        # Derived column
        assert row.total_amount == row.price * row.quantity * (1 - row.discount)

        # Year derived from order_date
        assert row.year == row.order_date.year

        # Join validations
        assert row.customer_name in ["Alice", "Bob"]
        assert row.country == "USA"
        assert row.category in ["Electronics", "Furniture"]
        assert row.sub_category in ["Laptop", "Chair"]