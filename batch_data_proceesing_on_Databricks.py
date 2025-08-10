   # Define the schema for the sales data
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

    sales_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("sale_date", DateType(), True)
    ])

    # Read the batch of sales data from a specific path
    sales_df = spark.read \
        .option("header", "true") \
        .schema(sales_schema) \
        .csv("/mnt/sales_data/daily_sales_2025-07-31.csv") # Example path for a daily batch

    # Example: Calculate total sales for each transaction
    sales_df_transformed = sales_df.withColumn("total_sale_amount", sales_df["quantity"] * sales_df["price"])

    # Example: Filter out invalid transactions (e.g., quantity <= 0)
    sales_df_cleaned = sales_df_transformed.filter(sales_df_transformed["quantity"] > 0)

# Writing to a delta table
    # Write the processed data to a Delta table
    sales_df_cleaned.write \
        .mode("append") \
        .format("delta") \
        .save("/mnt/delta_lake/sales_summary")

# Scheduling job on Databricks notebook or as a job