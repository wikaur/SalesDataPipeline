from pyspark.sql import DataFrame
from pyspark.sql.functions import col,count,sum as spark_sum
from typing import List
def drop_nulls(df: DataFrame, critical_cols: list) -> DataFrame:

    return df.dropna(subset=critical_cols)


def drop_duplicates(df: DataFrame, subset_cols: list) -> DataFrame:
   
    return df.dropDuplicates(subset=subset_cols)
def validate_primary_key(df: DataFrame, pk_cols: List[str]) -> DataFrame:

    null_counts = df.select(*[
        spark_sum(col(c).isNull().cast("int")).alias(c)
        for c in pk_cols
    ]).collect()[0]

    null_cols = [c for c in pk_cols if null_counts[c] > 0]
    if null_cols:
        raise ValueError(f"Primary key columns contain nulls: {null_cols}")

    total_rows = df.count()
    distinct_rows = df.select(*pk_cols).distinct().count()

    if total_rows != distinct_rows:
        raise ValueError(f"Duplicate rows found in primary key columns: {pk_cols}")

    return df
def filter_range(df, column_name: str, min_value: float):
    return df.filter(
        (col(column_name).isNotNull()) &
        (col(column_name) > min_value)
    )