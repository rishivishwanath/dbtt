from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.types import StructType, StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Bowler_Analytics_Batch") \
    .config("spark.jars", "/home/hadoop/Downloads/mysql-connector-j_9.3.0-1ubuntu25.04_all/usr/share/java/mysql-connector-j-9.3.0.jar").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define MySQL connection parameters
jdbc_url = "jdbc:mysql://localhost:3306/ipl_analytics"
table_name = "bowler"  # The table name where your bowler data is stored
properties = {
    "user": "dbt_user",
    "password": "dbt_password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read data from MySQL table
df_bowler = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

# Register temp view for Spark SQL
df_bowler.createOrReplaceTempView("bowler_data")

# ===============================================================
# 1. Top Wicket Taking Bowlers
# ===============================================================
query1 = """
SELECT 
    bowler,
    COUNT(wicket_type) AS wickets_taken
FROM 
    bowler_data
WHERE 
    wicket_type IS NOT NULL AND wicket_type != ''
GROUP BY 
    bowler
ORDER BY 
    wickets_taken DESC
"""

top_wicket_takers = spark.sql(query1)

# ===============================================================
# 2. Most Common Dismissal Types
# ===============================================================
query2 = """
SELECT 
    wicket_type,
    COUNT(*) AS count
FROM 
    bowler_data
WHERE 
    wicket_type IS NOT NULL AND wicket_type != ''
GROUP BY 
    wicket_type
ORDER BY 
    count DESC
"""

common_dismissal_types = spark.sql(query2)
# ===============================================================
# 3. Batsmen Dismissed by "Caught"
# ===============================================================
query3 = """
SELECT 
    player_dismissed AS batsman
FROM 
    bowler_data
WHERE 
    wicket_type = 'caught'
    AND player_dismissed IS NOT NULL
    AND player_dismissed != ''
"""

caught_batsmen = spark.sql(query3)

# ===============================================================
# Output results to console
# ===============================================================
print("=== Top Wicket Taking Bowlers ===")
top_wicket_takers.show(truncate=False)

print("=== Most Common Dismissal Types ===")
common_dismissal_types.show(truncate=False)
print("=== Batsmen Dismissed by 'Caught' ===")
caught_batsmen.show(truncate=False)

# Stop the Spark session
spark.stop()

