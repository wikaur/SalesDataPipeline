
from pyspark.sql.types import *

orders_schema = StructType([
    StructField("Customer_ID", StringType(), False),
    StructField("Discount", DoubleType(), True),
    StructField("Order_Date", StringType(), True),
    StructField("Order_ID", StringType(), False),
    StructField("Price", DoubleType(), True),
    StructField("Product_ID", StringType(), False),
    StructField("Profit", DoubleType(), True),
    StructField("Quantity", LongType(), True),
    StructField("Row_ID", LongType(), False),
    StructField("Ship_Date", StringType(), True),
    StructField("Ship_Mode", StringType(), True),
])




customers_schema = StructType([
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




products_schema = StructType([
    StructField("Product_ID", StringType(), False),
    StructField("Category", StringType(), False),
    StructField("Sub_Category", StringType(), False),
    StructField("Product_Name", StringType(), False),
    StructField("State", StringType(), True),
    StructField("Price_per_product", StringType(), False),  
])



