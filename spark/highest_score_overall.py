from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum
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
streaming_log_file = os.path.join(log_dir, "highest_score_streaming_log.csv")
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
