from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
from pyspark.sql.types import StructType, StringType

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

