from src.transforms.schema import *
from src.transforms.schema_validation import validate_schema

def test_orders_schema(spark):
    data = [("C001", 0.1, "2026-02-10", "O001", 100.0, "P001", 30.0, 2, 1, "2026-02-12", "Standard")]
    df = spark.createDataFrame(data, schema=orders_schema)  
    assert validate_schema(df, orders_schema)


def test_customers_schema(spark):
    data = [("C001","Alice","alice@mail.com","123456","Addr1","Retail","USA","New York","NY",10001,"East")]
    df = spark.createDataFrame(data, schema=customers_schema)  
    assert validate_schema(df, customers_schema)

def test_products_schema(spark):
    data = [("P001","Furniture","Chairs","Office Chair","NY","120.5")]
    df = spark.createDataFrame(data, schema=products_schema)  
    assert validate_schema(df, products_schema)