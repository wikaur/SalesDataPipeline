
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, round as spark_round

def aggregate_profit_by_customer_product(df_enriched: DataFrame) -> DataFrame:
   
    
    df_agg = (
        df_enriched
        .groupBy(
            "year",
            "customer_id",
            "customer_name",
            "category",
            "sub_category"
        )
        .agg(
            spark_round(spark_sum("profit"), 2).alias("total_profit")
        )
        .orderBy("year", "customer_id", "category", "sub_category")
    )

    return df_agg