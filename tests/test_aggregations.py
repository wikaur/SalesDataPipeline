from src.transforms.aggregations import *

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType,StringType,LongType,DateType
from datetime import date

def test_aggregate_profit_by_customer_product(spark):
   
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("country", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("category", StringType(), False),
        StructField("sub_category", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("quantity", LongType(), False),
        StructField("discount", DoubleType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("profit", DoubleType(), False),
        StructField("order_date", DateType(), False),
        StructField("ship_date", DateType(), False),
        StructField("year", LongType(), False),
    ])

    df_enriched = spark.createDataFrame([
        ("O1", "C1", "Alice", "USA", "P1", "Electronics", "Laptop", 100.0, 2, 0.1, 180.0, 20.1234, date(2023,1,1), date(2023,1,3), 2023),
        ("O2", "C1", "Alice", "USA", "P2", "Electronics", "Laptop", 150.0, 1, 0.05, 142.5, 30.567, date(2023,2,1), date(2023,2,3), 2023),
        ("O3", "C2", "Bob", "USA", "P3", "Furniture", "Chair", 50.0, 1, 0.0, 50.0, 5.789, date(2023,3,1), date(2023,3,3), 2023),
        ("O4", "C1", "Alice", "USA", "P4", "Electronics", "Phone", 200.0, 1, 0.1, 180.0, 25.432, date(2024,1,1), date(2024,1,2), 2024),
    ], schema=schema)

    
    df_agg = aggregate_profit_by_customer_product(df_enriched)
    rows = df_agg.collect()

   
    expected_combinations = [
        (2023, "C1", "Alice", "Electronics", "Laptop"),
        (2023, "C2", "Bob", "Furniture", "Chair"),
        (2024, "C1", "Alice", "Electronics", "Phone"),
    ]


    assert len(rows) == 3

   
    for row in rows:
        if row.customer_id == "C1" and row.category == "Electronics" and row.sub_category == "Laptop" and row.year == 2023:
            # Sum profit = 20.1234 + 30.567 = 50.69 after rounding
            assert row.total_profit == 50.69
        if row.customer_id == "C2" and row.category == "Furniture" and row.sub_category == "Chair":
            assert row.total_profit == 5.79
        if row.customer_id == "C1" and row.category == "Electronics" and row.sub_category == "Phone":
            assert row.total_profit == 25.43