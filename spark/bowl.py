from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, current_timestamp, window
from pyspark.sql.types import StructType, StringType
import time
import os
import csv
import psutil
import datetime
import atexit

# Create logs directory if it doesn't exist
log_dir = "performance_logs"
os.makedirs(log_dir, exist_ok=True)

# Initialize performance logger
streaming_log_file = os.path.join(log_dir, "bowl_streaming_log.csv")
# Create/open the log file with headers if it doesn't exist
if not os.path.exists(streaming_log_file):
    with open(streaming_log_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'program_name', 'processing_time_ms', 
                         'cpu_percent', 'memory_usage_mb'])

# Start time
start_time = time.time()

# Function to log performance at program exit
def log_performance_on_exit():
    # Get end time
    end_time = time.time()
    elapsed_time = (end_time - start_time) * 1000  # ms
    
    # Get CPU and memory usage
    cpu_percent = psutil.cpu_percent()
    memory_usage = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)  # MB
    
    # Write a single log entry for the entire program
    with open(streaming_log_file, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "Bowler_Analytics",
            elapsed_time,
            cpu_percent,
            memory_usage
        ])
    
    print(f"\nProgram Performance Log:")
    print(f"Total execution time: {elapsed_time:.2f} ms")
    print(f"CPU usage: {cpu_percent}%")
    print(f"Memory usage: {memory_usage:.2f} MB")

# Register the function to run at exit
atexit.register(log_performance_on_exit)

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
# Output to Console with Triggering
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
