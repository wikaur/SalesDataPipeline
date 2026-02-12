from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, expr
from src.transforms.utils import *
from src.transforms.dq import drop_nulls, drop_duplicates, validate_primary_key,filter_range

def silver_enriched_orders(df_orders: DataFrame) -> DataFrame:

    
    df_orders = clean_column_names(df_orders)

    
    df_orders = cast_column(df_orders, {
        "discount": "double",
        "price": "double",
        "profit": "double",
        "quantity": "long",
        "row_id": "long"
    })
    df_orders = cast_to_date(df_orders, ["order_date"])
    df_orders = cast_to_date(df_orders, ["ship_date"])

  
    df_orders = drop_nulls(df_orders, ["order_id", "customer_id", "product_id"])
    df_orders = validate_primary_key(df_orders, ["row_id"])
    df_orders = filter_range(df_orders, "price", 0)
    df_orders = filter_range(df_orders, "quantity", 0)
    df_orders = drop_duplicates(df_orders, ["row_id"])

  
    df_orders = df_orders.withColumn("year", year(col("order_date")))
    df_orders = df_orders.withColumn("total_amount", expr("price * quantity * (1 - discount)"))

    return df_orders


def silver_enriched_customers(df_customers: DataFrame) -> DataFrame:
   
    df_customers = clean_column_names(df_customers)

 
    df_customers = cast_column(df_customers, {"postal_code": "long"})

  
    df_customers = drop_nulls(df_customers, ["customer_id"])

  
    df_customers = validate_primary_key(df_customers, ["customer_id"])

    return df_customers


def silver_enriched_products(df_products: DataFrame) -> DataFrame:
    df_products = clean_column_names(df_products)

   
    df_products = cast_column(df_products, {"price_per_product": "double"})

  
    df_products = drop_nulls(df_products, ["product_id"])


    df_products = validate_primary_key(df_products, ["product_id","product_name","state"])

    return df_products



def silver_enriched_order_details(df_orders: DataFrame, df_customers: DataFrame, df_products: DataFrame) -> DataFrame:
   
  
    df_orders_silver = silver_enriched_orders(df_orders)
    df_customers_silver = silver_enriched_customers(df_customers)
    df_products_silver = silver_enriched_products(df_products)

   
    df = df_orders_silver.join(
        df_customers_silver.select("customer_id", "customer_name", "country"),
        on="customer_id",
        how="left"
    )

  
    df = df.join(
        df_products_silver.select("product_id", "category", "sub_category"),
        on="product_id",
        how="left"
    )

    df = df.fillna({"category": "Unknown", "sub_category": "Unknown"})
    df = df.withColumn("profit", spark_round(col("profit"), 2))

   
    df = df.select(
        "order_id",
        "customer_id",
        "customer_name",
        "country",
        "product_id",
        "category",
        "sub_category",
        "price",
        "quantity",
        "discount",
        "total_amount",
        "profit",
        "order_date",
        "ship_date",
        "year"
    )

    return df