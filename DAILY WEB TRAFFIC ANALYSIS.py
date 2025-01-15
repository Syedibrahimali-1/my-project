# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, expr
import random
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("GenerateSampleData").getOrCreate()

page_urls = ["/home", "/about", "/contact", "/products", "/services"]

def generate_data():
    data = []
    start_date = datetime(2025, 1, 1)
    for i in range(365):  # Generate data for 365 days
        date = start_date + timedelta(days=i)
        for page in page_urls:
            views = random.randint(50, 1000)  # Random views between 50 and 1000
            data.append((page, date, views))
    return data

columns = ["page_url", "visit_date", "views"]
data = generate_data()
web_traffic_df = spark.createDataFrame(data, columns)

web_traffic_df.show(10)

output_path = "/mnt/data/web_traffic.csv"
web_traffic_df.write.csv(output_path, header=True, mode="overwrite")


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

spark = SparkSession.builder.appName("WebTrafficAnalysis").getOrCreate()

# Define schema
schema = StructType([
    StructField("page_url", StringType(), True),
    StructField("visit_date", DateType(), True),
    StructField("views", IntegerType(), True)
])

# Load data (replace with your file path)
data_path = "/mnt/data/web_traffic.csv"
traffic_df = spark.read.csv(data_path, schema=schema, header=True)


# COMMAND ----------

cleaned_df = traffic_df.filter(traffic_df.page_url.isNotNull() & traffic_df.visit_date.isNotNull())


# COMMAND ----------

display(cleaned_df)

# COMMAND ----------

from pyspark.sql import functions as F

daily_traffic_df = cleaned_df.groupBy("page_url", "visit_date").agg(F.sum("views").alias("daily_views"))


# COMMAND ----------

display(daily_traffic_df)

# COMMAND ----------

from pyspark.sql.window import Window


window_spec = Window.partitionBy("page_url").orderBy("visit_date").rowsBetween(-6, 0)


moving_avg_df = daily_traffic_df.withColumn("7_day_moving_avg", F.avg("daily_views").over(window_spec))


# COMMAND ----------

display(moving_avg_df)

# COMMAND ----------

rank_window = Window.partitionBy("visit_date").orderBy(F.desc("daily_views"))

ranked_df = moving_avg_df.withColumn("rank", F.rank().over(rank_window))

top_pages_df = ranked_df.filter(ranked_df.rank <= 5)


# COMMAND ----------

display(top_pages_df)

# COMMAND ----------

output_path = "/mnt/data/delta/web_traffic_analysis"
top_pages_df.write.format("delta").mode("overwrite").save(output_path)

