from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lower, initcap,col, to_date, round as spark_round
from pyspark.sql.types import LongType, StructType, StructField, StringType
import re

def clean_trim(df: DataFrame, columns: list[str]) -> DataFrame:
    """Trim whitespace for specified columns."""
    for c in columns:
        df = df.withColumn(c, trim(col(c)))
    return df

def lowercase_column(df: DataFrame, column: str) -> DataFrame:
    """Convert a column to lowercase."""
    return df.withColumn(column, lower(col(column)))

def initcap_column(df: DataFrame, column: str) -> DataFrame:
    """Capitalize first letter of each word."""
    return df.withColumn(column, initcap(col(column)))

def cast_column(df: DataFrame, column: str, dtype) -> DataFrame:
    """Cast a column to a specific type."""
    return df.withColumn(column, col(column).cast(dtype))

def clean_column_names(df: DataFrame) -> DataFrame:
    def clean(col):
        col = col.strip().lower()
        col = re.sub(r"[^\w]+", "_", col)   # replace special chars with _
        col = re.sub(r"_+", "_", col)       # remove multiple _
        return col.strip("_")

    new_cols = [clean(c) for c in df.columns]
    return df.toDF(*new_cols)

def cast_to_date(df: DataFrame, columns: list[str], date_format: str = "yyyy-MM-dd") -> DataFrame:
    for c in columns:
        df = df.withColumn(c, to_date(col(c), date_format))
    return df


def round_column(df: DataFrame, column: str, scale: int = 2) -> DataFrame:
    return df.withColumn(column, spark_round(col(column), scale))


def remove_duplicates(df: DataFrame, subset: list[str]) -> DataFrame:
    return df.dropDuplicates(subset)


def rename_columns(df: DataFrame, rename_map: dict) -> DataFrame:
    for old, new in rename_map.items():
        df = df.withColumnRenamed(old, new)
    return df


def cast_column(df: DataFrame, column, data_type=None) -> DataFrame:

    
    if isinstance(column, dict):
        for col_name, dtype in column.items():
            df = df.withColumn(col_name, col(col_name).cast(dtype))
        return df

    if data_type is not None:
        return df.withColumn(column, col(column).cast(data_type))

    raise ValueError("Invalid arguments passed to cast_column")