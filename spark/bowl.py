from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, current_timestamp, window
from pyspark.sql.types import StructType, StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Bowler_Analytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema for bowler topic
schema = StructType() \
    .add("match_id", StringType()) \
    .add("batting_team", StringType()) \
    .add("bowling_team", StringType()) \
    .add("striker", StringType()) \
    .add("bowler", StringType()) \
    .add("wicket_type", StringType()) \
    .add("player_dismissed", StringType()) \
    .add("fielder", StringType())

# Read stream from Kafka topic 'bowler'
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bowler") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka value and extract fields
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Add is_wicket column
df_with_wickets = df_parsed.withColumn("is_wicket", col("wicket_type").isNotNull().cast("int"))

# ===============================================================
# 1. Top Wicket Taking Bowlers (wicket_type is not null)
# ===============================================================
wicket_counts = df_with_wickets.filter((col("is_wicket") == 1) & (col("wicket_type").isNotNull()) & (col("wicket_type") != "")) \
    .groupBy("bowler") \
    .agg(
        count("wicket_type").alias("wickets_taken")
    ) \
    .orderBy(col("wickets_taken").desc())

# ===============================================================
# 2. Most Common Dismissal Types (excluding empty or null values)
# ===============================================================
dismissal_types = df_with_wickets.filter((col("is_wicket") == 1) & (col("wicket_type").isNotNull()) & (col("wicket_type") != "")) \
    .groupBy("wicket_type") \
    .agg(count("*").alias("count")) \
    .orderBy(col("count").desc())
# ===============================================================
# 3. Batsmen Dismissed by "Caught"
# ===============================================================
caught_dismissals = df_with_wickets.filter(
    (col("is_wicket") == 1) & 
    (col("wicket_type") == "caught") & 
    (col("player_dismissed").isNotNull()) & 
    (col("player_dismissed") != "")
).select(
    col("player_dismissed").alias("batsman")
)
# ===============================================================
# Output to Console with Triggering Every Time Match Data Completes
# ===============================================================
# Query 1: Top wicket takers - Flushing after every match completes
query1 = wicket_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Top Wicket Takers") \
    .trigger(processingTime='5 seconds') \
    .start()

# Query 2: Most common dismissal types - Flushing after every match completes
query2 = dismissal_types.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Common Dismissal Types") \
    .trigger(processingTime='5 seconds') \
    .start()
query3 = caught_dismissals.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Caught Dismissals") \
    .start()
# Wait for all queries to finish
spark.streams.awaitAnyTermination()

