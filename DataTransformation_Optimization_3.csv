from pyspark.sql.functions import sum as _sum, count

# Aggregate data to find total purchase amount per customer
customer_purchase_df = complete_df.groupBy("customer_id", "name") \
    .agg(_sum("amount").alias("total_purchase"))

# Aggregate data to find total purchase amount per product
product_purchase_df = complete_df.groupBy("product_id", "product_name", "category") \
    .agg(_sum("amount").alias("total_sales"), count("transaction_id").alias("total_transactions"))

# Calculate total purchase amount over time
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("customer_id").orderBy("date")
complete_df = complete_df.withColumn("row_number", row_number().over(windowSpec))


# Caching DataFrames for repeated use
customer_purchase_df.cache()
product_purchase_df.cache()

# Using partitioning to optimize joins and aggregations
complete_df = complete_df.repartition("customer_id")

# Configuration tuning
spark.conf.set("spark.sql.shuffle.partitions", "50")
spark.conf.set("spark.executor.memory", "4g")

# write aggregated data to hive tables 
customer_purchase_df.write.mode("overwrite").saveAsTable("customer_purchase_summary")
product_purchase_df.write.mode("overwrite").saveAsTable("product_sales_summary")


# Query the Hive tables for analysis

# Top customers by purchase amount
top_customers = spark.sql("SELECT * FROM customer_purchase_summary ORDER BY total_purchase DESC LIMIT 10")
top_customers.show()

# Top products by sales amount
top_products = spark.sql("SELECT * FROM product_sales_summary ORDER BY total_sales DESC LIMIT 10")
top_products.show()
