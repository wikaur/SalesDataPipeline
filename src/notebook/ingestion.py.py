# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------


import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Add src to sys.path
import sys
sys.path.append('/Workspace/Users/wishi.kaur3@gmail.com/PEI_Assessment/src')

# COMMAND ----------

# DBTITLE 1,Add parent directory to sys.path
import sys
sys.path.append('/Workspace/Users/wishi.kaur3@gmail.com/PEI_Assessment')

# COMMAND ----------




customers_raw_df = (
    spark.read
    .format("excel")
    .option("headerRows", 1)
    .load("/Volumes/assessment/pei/data/Customer.xlsx")
)

display(customers_raw_df)


# COMMAND ----------

orders_raw_df=spark.read.format('json').option("multiline", "true").load('/Volumes/assessment/pei/data/Orders.json')
display(orders_raw_df)

# COMMAND ----------

products_df=spark.read.option('header',True).option("quote", '"') \
    .option("escape", '"').csv('/Volumes/assessment/pei/data/Products.csv')
display(products_df)

# COMMAND ----------

from src.transforms.bronze import *
from src.transforms.silver import *
from src.transforms.utils import *
from src.transforms.dq import *
from src.transforms.aggregations import aggregate_profit_by_customer_product

# COMMAND ----------

df_orders_bronze = bronze_ingest(orders_raw_df, "orders")
df_customers_bronze = bronze_ingest(customers_raw_df, "customers")
df_products_bronze = bronze_ingest(products_df, "products")

# COMMAND ----------

# DBTITLE 1,Untitled
df_orders_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("assessment.pei.bronze_orders")

df_customers_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("assessment.pei.bronze_customers")

df_products_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("assessment.pei.bronze_products")


# COMMAND ----------

df_orders_bronze = spark.table("assessment.pei.bronze_orders")
df_customers_bronze = spark.table("assessment.pei.bronze_customers")
df_products_bronze = spark.table("assessment.pei.bronze_products")

# COMMAND ----------

df_orders_silver = silver_enriched_orders(df_orders_bronze)
df_customers_silver = silver_enriched_customers(df_customers_bronze)
df_products_silver = silver_enriched_products(df_products_bronze)

df_order_details_silver = silver_enriched_order_details(
    df_orders_bronze,
    df_customers_bronze,
    df_products_bronze
)

# COMMAND ----------

df_orders_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("assessment.pei.silver_orders")

df_customers_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("assessment.pei.silver_customers")

df_products_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("assessment.pei.silver_products")

df_order_details_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("assessment.pei.silver_order_details")


# COMMAND ----------

df_aggregated_gold = aggregate_profit_by_customer_product(df_order_details_silver)

# COMMAND ----------

df_aggregated_gold.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("assessment.pei.gold_aggregate_profit")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Profit by year
# MAGIC SELECT 
# MAGIC   year,
# MAGIC     ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM assessment.pei.gold_aggregate_profit
# MAGIC GROUP BY year
# MAGIC ORDER BY year;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Profit by year,product_category
# MAGIC SELECT 
# MAGIC     year,
# MAGIC     category,
# MAGIC     ROUND(SUM(total_profit), 2) AS profit_by_year_category
# MAGIC FROM assessment.pei.gold_aggregate_profit
# MAGIC GROUP BY year, category
# MAGIC ORDER BY year, category;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --Profit by customer
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     ROUND(SUM(total_profit), 2) AS profit_by_customer
# MAGIC FROM assessment.pei.gold_aggregate_profit
# MAGIC GROUP BY customer_id
# MAGIC ORDER BY profit_by_customer DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --Profit by customer_id,year
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     year,
# MAGIC     ROUND(SUM(total_profit), 2) AS profit_by_customer_year
# MAGIC FROM assessment.pei.gold_aggregate_profit
# MAGIC GROUP BY customer_id, year
# MAGIC ORDER BY customer_id, year;
# MAGIC