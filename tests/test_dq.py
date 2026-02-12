from src.transforms.dq import *
import pytest
from pyspark.sql.types import *
def test_drop_nulls(spark):
    
    df = spark.createDataFrame([
        (1, "A"),
        (2, None),
        (None, "B")
    ], ["id", "name"])

    result = drop_nulls(df, ["id", "name"])
    assert result.count() == 1
    row = result.collect()[0]
    assert row["id"] == 1 and row["name"] == "A"

def test_drop_duplicates(spark):
    df = spark.createDataFrame([
        (1, "A"),
        (1, "A"),
        (2, "B")
    ], ["id", "name"])

    result = drop_duplicates(df, ["id", "name"])
    assert result.count() == 2

def test_validate_primary_key_success(spark):
    df = spark.createDataFrame([
        (1, "A"),
        (2, "B"),
        (3, "C")
    ], ["id", "name"])
    result = validate_primary_key(df, ["id"])
    assert result.count() == 3

def test_validate_primary_key_null(spark):
    df = spark.createDataFrame([
        (1, "A"),
        (None, "B")
    ], ["id", "name"])
    with pytest.raises(ValueError):
        validate_primary_key(df, ["id"])

def test_validate_primary_key_duplicate(spark):
    df = spark.createDataFrame([
        (1, "A"),
        (1, "B")
    ], ["id", "name"])
    with pytest.raises(ValueError):
        validate_primary_key(df, ["id"])

def test_validate_primary_key_composite(spark):
    df = spark.createDataFrame([
        (1, "A"),
        (1, "B"),
        (1, "B")
    ], ["id", "type"])
    with pytest.raises(ValueError):
        validate_primary_key(df, ["id", "type"])

def test_filter_range_price_quantity_profit(spark):

    schema = StructType([
        StructField("price", DoubleType(), True),
        StructField("profit", DoubleType(), True),
        StructField("quantity", LongType(), True),
    ])

    df = spark.createDataFrame([
        (100.0, 20.0, 2),     # valid
        (-10.0, 5.0, 1),      #  negative price
        (50.0, -5.0, 1),      # negative profit
        (50.0, 5.0, -1),      # negative quantity
        (None, 5.0, 1),       # null price
        (50.0, None, 1),      # null profit
        (50.0, 5.0, None),    #  null quantity
    ], schema=schema)

   
    df_filtered = filter_range(df, "price", 0)
    df_filtered = filter_range(df_filtered, "profit", 0)
    df_filtered = filter_range(df_filtered, "quantity", 0)

    result = df_filtered.collect()

    assert len(result) == 1
    assert result[0]["price"] == 100.0
    assert result[0]["profit"] == 20.0
    assert result[0]["quantity"] == 2