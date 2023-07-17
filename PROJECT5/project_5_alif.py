from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.functions import date_format, max, desc, col

spark = SparkSession.builder.appName("project_5_alif").getOrCreate()

#read the file
df= spark.read.parquet('/home/dev/airflow/spark-code/Project-5/fhv_tripdata_2021-02.parquet')
#show
df.show(10)

# 1. How many taxi trips were there on February 15?
filtered_df = df.filter(df["pickup_datetime"].startswith("2021-02-15")).count()
print(f'\nHow many taxi trips on February 15 = {filtered_df}\n')

# 2. Find the longest trip for each day?
df = df.withColumn("pickup_date", date_format("pickup_datetime", "yyyy-MM-dd"))
longest_trips = df.groupBy("pickup_date").agg(max("pickup_datetime").alias("max_trip_distance"))
longest_trips.show()

# 3. Find Top 5 Most frequent `dispatching_base_num`?
dbc = df.groupBy("dispatching_base_num").agg(count("*").alias("count"))
sorted_dbc = dbc.orderBy(desc("count"))
top_5_dbc = sorted_dbc.limit(5).show()

# 4. Find Top 5 Most common location pairs (PUlocationID and DOlocationID?
filtered_df = df.filter((col("PUlocationID").isNotNull()) & (col("DOlocationID").isNotNull()))
lpc = filtered_df.groupBy("PUlocationID", "DOlocationID").agg(count("*").alias("count"))
sorted_lpc = lpc.orderBy(desc("count")).limit(5).show()