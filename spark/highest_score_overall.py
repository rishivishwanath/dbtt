from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum
from pyspark.sql.types import StructType, StringType

# Create Spark Session
spark = SparkSession.builder \
    .appName("IPL_Highest_Scorers") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming Kafka data
schema = StructType() \
    .add("match_id", StringType()) \
    .add("batting_team", StringType()) \
    .add("innings", StringType()) \
    .add("over", StringType()) \
    .add("striker", StringType()) \
    .add("runs_of_bat", StringType())

# Read stream from Kafka topic 'runs'
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "runs") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse and cast data
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("runs_of_bat", col("runs_of_bat").cast("int")) \
    .withColumn("innings", col("innings").cast("int"))

# =====================================
# 2. Highest overall scorers
# =====================================
highest_scorers = df_parsed.groupBy("striker","batting_team") \
    .agg(_sum("runs_of_bat").alias("total_runs")) \
    .orderBy(col("total_runs").desc())

# =====================================
# Output to Console
# =====================================


query1 = highest_scorers.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Overall Highest Scorers") \
    .start()

spark.streams.awaitAnyTermination()

