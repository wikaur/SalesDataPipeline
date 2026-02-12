import pytest
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import date
from src.transforms.silver import silver_enriched_orders,silver_enriched_customers,silver_enriched_products,silver_enriched_order_details

def test_silver_enriched_orders(spark):
   
    schema = StructType([
        StructField("Customer_ID", StringType(), False),
        StructField("Discount", StringType(), True),
        StructField("Order_Date", StringType(), True),
        StructField("Order_ID", StringType(), False),
        StructField("Price", StringType(), True),
        StructField("Product_ID", StringType(), False),
        StructField("Profit", StringType(), True),
        StructField("Quantity", StringType(), True),
        StructField("Row_ID", StringType(), False),
        StructField("Ship_Date", StringType(), True),
        StructField("Ship_Mode", StringType(), True),
    ])


    df_orders = spark.createDataFrame([
        ("C1", "0.1", "2023-01-01", "O1", "100", "P1", "20", "2", "1", "2023-01-03", "Standard"),
        ("C2", "0.0", "2023-01-02", "O2", "-50", "P2", "5", "1", "2", "2023-01-04", "Express"),
        ("C4", "0.0", "2023-01-04", "O4", "60", "P4", "10", "0", "4", "2023-01-06", "Express"),
    ], schema=schema)

    
    result = silver_enriched_orders(df_orders)
    rows = result.collect()

   
    assert all(r.order_id in ["O1"] for r in rows)

   
    expected_cols = [
        "order_id", "customer_id", "product_id",
        "price", "profit", "quantity", "discount", "row_id",
        "order_date", "ship_date", "year", "total_amount"
    ]
    for col_name in expected_cols:
        assert col_name in result.columns

    
    for row in rows:
        
        assert isinstance(row.price, float)
        assert isinstance(row.profit, float)
        assert isinstance(row.quantity, int)
        assert isinstance(row.discount, float)
        assert isinstance(row.row_id, int)


        assert isinstance(row.order_date, date)
        assert isinstance(row.ship_date, date)


        assert row.year == 2023
        assert row.total_amount == row.price * row.quantity * (1 - row.discount)

   
    row_o1 = rows[0]
    assert row_o1.order_id == "O1"
    assert row_o1.customer_id == "C1"
    assert row_o1.product_id == "P1"
    assert row_o1.total_amount == 180.0  # 100 * 2 * (1 - 0.1)

def test_silver_enriched_customers(spark):
    schema = StructType([
        StructField("Customer_ID", StringType(), False),
        StructField("Customer_Name", StringType(), False),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("Segment", StringType(), True),
        StructField("Country", StringType(), False),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Postal_Code", LongType(), True),
        StructField("Region", StringType(), True),
    ])

    df_customers = spark.createDataFrame([
        ("C1", "Alice", "alice@email.com", "12345", "Addr1", "Consumer", "USA", "NY", "NY", 10001, "East"),
        ("C2", "Bob", None, None, None, None, "USA", "CA", "CA", 90001, "West"),
        ("C3", "Charlie", "charlie@email.com", "67890", "Addr3", "Corporate", "USA", "TX", "TX", None, "South"),
    ], schema=schema)

    result = silver_enriched_customers(df_customers)
    rows = result.collect()

   
    assert all(r.customer_id is not None for r in rows)

   
    for col_name in ["customer_id", "customer_name", "email", "phone", "address", "segment",
                     "country", "city", "state", "postal_code", "region"]:
        assert col_name in result.columns

  
    for r in rows:
        if r.postal_code is not None:
            assert isinstance(r.postal_code, int)

def test_silver_enriched_products(spark):
    schema = StructType([
        StructField("Product_ID", StringType(), False),
        StructField("Category", StringType(), False),
        StructField("Sub_Category", StringType(), False),
        StructField("Product_Name", StringType(), False),
        StructField("State", StringType(), True),
        StructField("Price_per_product", StringType(), False),
    ])

    df_products = spark.createDataFrame([
        ("P1", "Electronics", "Laptop", "Laptop A", "NY", "1000"),
        ("P2", "Furniture", "Chair", "Chair B", "CA", "200"),
        ("P3", "Electronics", "Phone", "Phone C", "TX", "500"),
    ], schema=schema)

    result = silver_enriched_products(df_products)
    rows = result.collect()


    for col_name in ["product_id", "category", "sub_category", "product_name", "state", "price_per_product"]:
        assert col_name in result.columns

    assert all(r.product_id is not None for r in rows)

   
    for r in rows:
        assert isinstance(r.price_per_product, float)

def test_silver_enriched_order_details(spark):
  
    orders_schema = StructType([
        StructField("Customer_ID", StringType(), False),
        StructField("Discount", StringType(), True),
        StructField("Order_Date", StringType(), True),
        StructField("Order_ID", StringType(), False),
        StructField("Price", StringType(), True),
        StructField("Product_ID", StringType(), False),
        StructField("Profit", StringType(), True),
        StructField("Quantity", StringType(), True),
        StructField("Row_ID", StringType(), False),
        StructField("Ship_Date", StringType(), True),
        StructField("Ship_Mode", StringType(), True),
    ])

    df_orders = spark.createDataFrame([
        ("C1", "0.1", "2023-01-01", "O1", "100", "P1", "20.1234", "2", "1", "2023-01-03", "Standard"),
        ("C2", "0.0", "2023-01-02", "O2", "50", "P2", "5.567", "1", "2", "2023-01-04", "Express"),
    ], schema=orders_schema)


    customers_schema = StructType([
        StructField("Customer_ID", StringType(), False),
        StructField("Customer_Name", StringType(), False),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("Segment", StringType(), True),
        StructField("Country", StringType(), False),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Postal_Code", LongType(), True),
        StructField("Region", StringType(), True),
    ])

    df_customers = spark.createDataFrame([
        ("C1", "Alice", "a@mail.com", None, None, None, "USA", None, None, 10001, None),
        ("C2", "Bob", "b@mail.com", None, None, None, "USA", None, None, 90001, None),
    ], schema=customers_schema)

    
    products_schema = StructType([
        StructField("Product_ID", StringType(), False),
        StructField("Category", StringType(), False),
        StructField("Sub_Category", StringType(), False),
        StructField("Product_Name", StringType(), False),
        StructField("State", StringType(), True),
        StructField("Price_per_product", StringType(), False),
    ])

    df_products = spark.createDataFrame([
        ("P1", "Electronics", "Laptop", "Laptop A", "NY", "1000"),
        ("P2", "Furniture", "Chair", "Chair B", "CA", "200"),
    ], schema=products_schema)


    result = silver_enriched_order_details(df_orders, df_customers, df_products)
    rows = result.collect()

    # There should be 2 rows
    assert len(rows) == 2

    # Check column names
    expected_cols = [
        "order_id", "customer_id", "customer_name", "country",
        "product_id", "category", "sub_category",
        "price", "quantity", "discount", "total_amount", "profit",
        "order_date", "ship_date", "year"
    ]
    for col_name in expected_cols:
        assert col_name in result.columns

    # Validate types and values
    for row in rows:
        # profit rounded
        assert round(row.profit, 2) == row.profit
        # Derived column total_amount
        assert row.total_amount == row.price * row.quantity * (1 - row.discount)
        # Year
        assert row.year == row.order_date.year
        # Customer info
        assert row.customer_name in ["Alice", "Bob"]
        assert row.country == "USA"
        # Product info
        assert row.category in ["Electronics", "Furniture"]
        assert row.sub_category in ["Laptop", "Chair"]
