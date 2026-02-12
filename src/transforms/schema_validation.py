from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

def validate_schema(df: DataFrame, expected_schema: StructType, raise_error=True) -> bool:

    df_schema = df.schema

   
    if len(df_schema.fields) != len(expected_schema.fields):
        if raise_error:
            raise AssertionError(f"Number of columns mismatch: {len(df_schema.fields)} != {len(expected_schema.fields)}")
        return False

   
    for actual_field, expected_field in zip(df_schema.fields, expected_schema.fields):
        if actual_field.name != expected_field.name:
            if raise_error:
                raise AssertionError(f"Column name mismatch: {actual_field.name} != {expected_field.name}")
            return False
        if type(actual_field.dataType) != type(expected_field.dataType):
            if raise_error:
                raise AssertionError(f"Data type mismatch for column '{actual_field.name}': {actual_field.dataType} != {expected_field.dataType}")
            return False
        if actual_field.nullable != expected_field.nullable:
            if raise_error:
                raise AssertionError(f"Nullable mismatch for column '{actual_field.name}': {actual_field.nullable} != {expected_field.nullable}")
            return False

    return True
