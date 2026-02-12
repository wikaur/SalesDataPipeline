from src.transforms.bronze import bronze_ingest

def test_bronze_ingest(spark):
    df = spark.createDataFrame([
        ("C1", "O1", 100.0)
    ], ["customer_id", "order_id", "price"])

    df_bronze = bronze_ingest(df, source_system="ERP")
    cols = df_bronze.columns

   
    assert "ingestion_time" in cols
    assert "source_system" in cols

    
    val = df_bronze.select("source_system").collect()[0][0]
    assert val == "ERP"