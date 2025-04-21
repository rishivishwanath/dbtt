from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as sum_col
from pyspark.sql.types import StructType, StringType
import logging
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
streaming_log_file = os.path.join(log_dir, "highest_score_streaming_match_log.csv")

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
            "IPL_Highest_Scorers",
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

# Suppress specific warnings
logging.getLogger("org.apache.spark.sql.execution.streaming.state").setLevel(logging.ERROR)

# Create Spark Session with more logging control
spark = SparkSession.builder \
    .appName("IPL_Highest_Scorers") \
    .config("spark.sql.streaming.metricsEnabled", "false") \
    .getOrCreate()

# Set log level to reduce verbose output
spark.sparkContext.setLogLevel("ERROR")

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
# 1. Calculate basic player scoring stats
# =====================================
scorers_by_match_innings = df_parsed.groupBy("match_id", "batting_team", "innings", "striker") \
    .agg(sum_col("runs_of_bat").alias("total_runs"))

# Write all player scores to console
#query1 = scorers_by_match_innings.writeStream \
 #   .outputMode("complete") \
  #  .format("console") \
   # .option("truncate", False) \
    #.option("numRows", 20).queryName("All Player Scores") \
    #.start()

# =====================================
# 2. Process each batch to find top scorers
# =====================================
def process_batch(batch_df, batch_id):
    if not batch_df.isEmpty():
        # Find max runs per group
        max_runs = batch_df.groupBy("match_id", "batting_team", "innings") \
            .agg({"total_runs": "max"}) \
            .withColumnRenamed("max(total_runs)", "max_runs")
        
        # Join to get top scorers
        top_scorers = batch_df.join(
            max_runs,
            on=["match_id", "batting_team", "innings"]
        ).where(
            batch_df.total_runs == max_runs.max_runs
        ).select(
            batch_df.match_id, 
            batch_df.batting_team,
            batch_df.innings,
            batch_df.striker,
            batch_df.total_runs
        ).orderBy("match_id", "batting_team", "innings")
        
        print("\n===== TOP SCORERS BY MATCH/INNINGS/TEAM =====")
        top_scorers.show(50, truncate=False)  # Show up to 50 rows

# Process each micro-batch as a separate static DataFrame
query2 = scorers_by_match_innings.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_batch) \
    .queryName("Top Scorers Processing") \
    .trigger(processingTime='5 seconds') \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()
