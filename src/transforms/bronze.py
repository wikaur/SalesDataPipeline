from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit
from src.transforms.utils import *

def bronze_ingest(df: DataFrame, source_system: str = "raw") -> DataFrame:
    df = clean_column_names(df)
    return df.withColumn("ingestion_time", current_timestamp()) \
             .withColumn("source_system", lit(source_system))