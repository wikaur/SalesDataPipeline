# conftest.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("pytest-spark")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    yield spark

    spark.stop()


import pytest
from pyspark.sql.types import *

@pytest.fixture
def orders_df(spark):
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

    return spark.createDataFrame([
        ("C1", "0.1", "2023-01-01", "O1", "100", "P1", "20", "2", "1", "2023-01-03", "Standard"),
        ("C2", "0.0", "2023-01-02", "O2", "-50", "P2", "5", "1", "2", "2023-01-04", "Express"),
        ("C4", "0.0", "2023-01-04", "O4", "60", "P4", "10", "0", "4", "2023-01-06", "Express"),
    ], schema)

@pytest.fixture
def customers_df(spark):

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

    return spark.createDataFrame([
        ("C1", "Alice", "alice@email.com", "12345", "Addr1", "Consumer", "USA", "NY", "NY", 10001, "East"),
        ("C2", "Bob", None, None, None, None, "USA", "CA", "CA", 90001, "West"),
        ("C3", "Charlie", "charlie@email.com", "67890", "Addr3", "Corporate", "USA", "TX", "TX", None, "South"),
    ], schema)


@pytest.fixture
def products_df(spark):

    schema = StructType([
        StructField("Product_ID", StringType(), False),
        StructField("Category", StringType(), False),
        StructField("Sub_Category", StringType(), False),
        StructField("Product_Name", StringType(), False),
        StructField("State", StringType(), True),
        StructField("Price_per_product", StringType(), False),
    ])

    return spark.createDataFrame([
        ("P1", "Electronics", "Laptop", "Laptop A", "NY", "1000"),
        ("P2", "Furniture", "Chair", "Chair B", "CA", "200"),
        ("P3", "Electronics", "Phone", "Phone C", "TX", "500"),
    ], schema)



@pytest.fixture
def orders_details_df(spark):

   

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

    return spark.createDataFrame([
        ("C1", "0.1", "2023-01-01", "O1", "100", "P1", "20.1234", "2", "1", "2023-01-03", "Standard"),
        ("C2", "0.0", "2023-01-02", "O2", "50", "P2", "5.567", "1", "2", "2023-01-04", "Express"),
    ], schema)

