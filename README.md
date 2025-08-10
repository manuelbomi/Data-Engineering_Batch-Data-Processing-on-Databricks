# Data Engineering: Batch Data Processing on Databricks

### Brief Overview

To save cost on cloud platforms, it is often a good alternative to process data in batches instead of continuously streaming data. A common example of batch data processing on Databricks involves processing historical data files, such as daily sales reports or log files, and transforming them into a structured format for analysis.

#### An example of batch data processing is Processing Daily Sales Data.
Consider a scenario where a retail company receives daily sales data in CSV files, stored in a cloud storage location (e.g., AWS S3, Azure Data Lake Storage, Google Cloud Storage). The goal is to process these daily batches, clean the data, enrich it, and store it in a Delta table for further analysis. Reading the Batch Data.

#### To process data in batches on Databricks,  Spark's read API can be used to load the CSV files into a DataFrame. The Data Engineer can specify options like header, schema inference, and delimiter. A good starting point is to specify the schema for storing or proceesing the data. 
---
```ruby
        # Define the schema for the sales data
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

    sales_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("sale_date", DateType(), True)
    ])
```
---

#### Processing the Kafka Stream. The raw Kafka messages (often in binary format) are typically parsed and transformed. For instance, if the messages are JSON strings, they can be parsed into a structured schema.
---
```ruby
    from pyspark.sql.functions import from_json, col
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    # Define your schema based on the expected JSON data structure. Inferschema is not used in this instance.
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("value", IntegerType())
    ])
```
---

####   # Read the batch of sales data from a specific path
---
```ruby
   sales_df = spark.read \
        .option("header", "true") \
        .schema(sales_schema) \
        .csv("/mnt/sales_data/daily_sales_2025-07-31.csv") # Example path for a daily batch
```
---

#### The next step is to ensure adequate data quality by transformation and cleaning the data. Transformations can include handling missing values, standardizing data formats, or adding new calculated columns; etc
---
```ruby
    # Example: Calculate total sales for each transaction
    sales_df_transformed = sales_df.withColumn("total_sale_amount", sales_df["quantity"] * sales_df["price"])

    # Example: Filter out invalid transactions (e.g., quantity <= 0)
    sales_df_cleaned = sales_df_transformed.filter(sales_df_transformed["quantity"] > 0)

```
---

#### Writing to a Delta Table. The Delta Table offers ACID properties, schema enforcement, and versioning.
---
```ruby
       # Write the processed data to a Delta table
    sales_df_cleaned.write \
        .mode("append") \
        .format("delta") \
        .save("/mnt/delta_lake/sales_summary")

```
---

#### Scheduling the Batch Job.
---
This entire process would typically be encapsulated within a Databricks notebook or job and scheduled to run daily using Databricks Jobs, ensuring the batch processing occurs automatically at a predefined interval.
---

Author's Background

```
> [!NOTE]
Author's Name:  Emmanuel Oyekanlu
Skillset:   I have experience spanning several years in developing scalable enterprise data pipelines,
solution architecture, architecting enterprise data and AI solutions, deep learning and LLM applications as
well as deploying solutions (apps) on-prem and in the cloud.

I can be reached through: manuelbomi@yahoo.com
Website:  http://emmanueloyekanlu.com/
Publications:  https://scholar.google.com/citations?user=S-jTMfkAAAAJ&hl=en
LinkedIn:  https://www.linkedin.com/in/emmanuel-oyekanlu-6ba98616
Github:  https://github.com/manuelbomi

```

[![Icons](https://skillicons.dev/icons?i=aws,azure,gcp,scala,mongodb,redis,cassandra,kafka,anaconda,matlab,nodejs,django,py,c,anaconda,git,github,mysql,docker,kubernetes&theme=dark)](https://skillicons.dev)







