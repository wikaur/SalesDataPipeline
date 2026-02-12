from src.transforms.utils import *

from pyspark.sql.types import DateType,DoubleType


def test_clean_trim_single_column(spark):
    df = spark.createDataFrame([("  Alice  ",)], ["name"])
    result = clean_trim(df, ["name"])

    assert result.collect()[0]["name"] == "Alice"


def test_clean_trim_multiple_columns(spark):
    df = spark.createDataFrame([("  Alice  ", "  NY ")], ["name", "city"])
    result = clean_trim(df, ["name", "city"])
    row = result.collect()[0]

    assert row["name"] == "Alice"
    assert row["city"] == "NY"



def test_lowercase_column(spark):
    df = spark.createDataFrame([("JOHN@MAIL.COM",)], ["email"])
    result = lowercase_column(df, "email")

    assert result.collect()[0]["email"] == "john@mail.com"



def test_initcap_column(spark):
    df = spark.createDataFrame([("john doe",)], ["name"])
    result = initcap_column(df, "name")

    assert result.collect()[0]["name"] == "John Doe"


def test_cast_column_to_long(spark):
    df = spark.createDataFrame([("100",)], ["amount"])
    result = cast_column(df, "amount", LongType())

    value = result.collect()[0]["amount"]

    assert isinstance(value, int)
    assert value == 100



def test_trim_with_null_value(spark):
    schema = StructType([
        StructField("name", StringType(), True)
    ])
    df = spark.createDataFrame([(None,)], schema)

    result = clean_trim(df, ["name"])
    assert result.collect()[0]["name"] is None


def test_lowercase_with_null(spark):
    schema = StructType([
        StructField("email", StringType(), True)
    ])
    df = spark.createDataFrame([(None,)], schema)

    result = lowercase_column(df, "email")
    assert result.collect()[0]["email"] is None


def test_clean_column_names(spark):
   
    df = spark.createDataFrame(
        [(1, 2)],
        ["Product ID", "Price per product"]
    )
    result = clean_column_names(df)
    assert "product_id" in result.columns
    assert "price_per_product" in result.columns
    assert len(result.columns) == 2

   
    row = result.collect()[0]
    assert row["product_id"] == 1
    assert row["price_per_product"] == 2



def test_cast_to_date(spark):
    df = spark.createDataFrame(
        [("2023-01-01",)],
        ["Order_Date"]
    )

    result = cast_to_date(df, ["Order_Date"])

    assert isinstance(result.schema["Order_Date"].dataType, DateType)

def test_round_column(spark):
    df = spark.createDataFrame([(10.456,)], ["Profit"])

    result = round_column(df, "Profit", 2)

    assert result.collect()[0]["Profit"] == 10.46

def test_remove_duplicates(spark):
    df = spark.createDataFrame(
        [("A",), ("A",), ("B",)],
        ["Order_ID"]
    )

    result = remove_duplicates(df, ["Order_ID"])

    assert result.count() == 2

def test_rename_columns(spark):
    df = spark.createDataFrame(
        [(1,)],
        ["Old_Column"]
    )

    result = rename_columns(df, {"Old_Column": "New_Column"})

    assert "New_Column" in result.columns


def test_cast_column(spark):
    df = spark.createDataFrame(
        [("10.5",)],
        ["Price"]
    )

    result = cast_column(df, "Price", DoubleType())

    assert isinstance(result.schema["Price"].dataType, DoubleType)