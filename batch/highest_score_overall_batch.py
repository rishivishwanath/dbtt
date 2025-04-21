from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
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
batch_log_file = os.path.join(log_dir, "highest_score_batch_log.csv")

# Create/open the log file with headers if it doesn't exist
if not os.path.exists(batch_log_file):
    with open(batch_log_file, 'w', newline='') as f:
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
    with open(batch_log_file, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "IPL_Highest_Scorers_Batch",
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
    .appName("IPL_Highest_Scorers_Batch") \
    .config("spark.jars", "/home/hadoop/Downloads/mysql-connector-j_9.3.0-1ubuntu25.04_all/usr/share/java/mysql-connector-j-9.3.0.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define MySQL connection parameters
jdbc_url = "jdbc:mysql://localhost:3306/ipl_analytics"
table_name = "runs"  # The table name where your runs data is stored
properties = {
    "user": "dbt_user",
    "password": "dbt_password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read data from MySQL table (for batch processing)
df_raw = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

# Register temp view for Spark SQL
df_raw.createOrReplaceTempView("runs_data")

# ===============================================================
# 1. Highest Overall Scorers
# ===============================================================
query1 = """
SELECT 
    striker,
    batting_team,
    SUM(CAST(runs_of_bat AS INT)) AS total_runs
FROM 
    runs_data
GROUP BY 
    striker, batting_team
ORDER BY 
    total_runs DESC
"""

highest_scorers = spark.sql(query1)

# ===============================================================
# Output results to console
# ===============================================================
print("=== Highest Overall Scorers ===")
highest_scorers.show(truncate=False)

# Stop the Spark session
spark.stop()
