from pyspark.sql import SparkSession
import logging

# Suppress specific warnings
logging.getLogger("org.apache.spark.sql.execution").setLevel(logging.ERROR)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("IPL_RunRate_Analysis_Batch") \
    .config("spark.jars", "/home/hadoop/Downloads/mysql-connector-j_9.3.0-1ubuntu25.04_all/usr/share/java/mysql-connector-j-9.3.0.jar") \
    .getOrCreate()

# Set log level to reduce verbose output
spark.sparkContext.setLogLevel("ERROR")

# Define MySQL connection parameters
jdbc_url = "jdbc:mysql://localhost:3306/ipl_analytics"
table_name = "rate"  # The table name where your run rate data is stored
properties = {
    "user": "dbt_user",
    "password": "dbt_password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read data from MySQL table (for batch processing)
df_raw = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

# Register temp view for Spark SQL
df_raw.createOrReplaceTempView("rate_data")

# Create a pre-processed view with proper casting and calculated fields
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW processed_data AS
SELECT
    match_id,
    batting_team,
    CAST(innings AS INT) AS innings,
    CAST(over AS FLOAT) AS over,
    striker,
    CAST(runs_of_bat AS INT) AS runs_of_bat,
    CAST(extras AS INT) AS extras,
    CAST(runs_of_bat AS INT) + CAST(extras AS INT) AS total_runs,
    1 AS balls
FROM
    rate_data
""")

# ==================================================
# 1. Run rate grouped by match_id, innings, team
# ==================================================
query1 = """
SELECT
    match_id,
    innings,
    batting_team,
    SUM(total_runs) AS total_runs,
    SUM(balls) AS total_balls,
    ROUND((SUM(total_runs) / SUM(balls)) * 6, 2) AS run_rate
FROM
    processed_data
GROUP BY
    match_id, innings, batting_team
ORDER BY
    match_id, innings
"""

runrate_by_match = spark.sql(query1)

print("\n===== RUN RATE BY MATCH, INNINGS, TEAM =====")
runrate_by_match.show(truncate=False)

# ==================================================
# 2. Overall run rate of each team (across matches)
# ==================================================
query2 = """
SELECT
    batting_team,
    SUM(total_runs) AS total_runs,
    SUM(balls) AS total_balls,
    ROUND((SUM(total_runs) / SUM(balls)) * 6, 2) AS overall_run_rate
FROM
    processed_data
GROUP BY
    batting_team
ORDER BY
    overall_run_rate DESC
"""

overall_team_runrate = spark.sql(query2)

print("\n===== OVERALL TEAM RUN RATE =====")
overall_team_runrate.show(truncate=False)

# ==================================================
# 3. Run rate per striker (player)
# ==================================================
query3 = """
SELECT
    striker,
    SUM(total_runs) AS total_runs,
    SUM(balls) AS balls_faced,
    ROUND((SUM(total_runs) / SUM(balls)) * 6, 2) AS run_rate
FROM
    processed_data
GROUP BY
    striker
ORDER BY
    run_rate DESC
"""

striker_runrate = spark.sql(query3)

print("\n===== RUN RATE PER STRIKER =====")
striker_runrate.show(truncate=False)

# ==================================================
# 4. Run rate in Powerplay (overs < 6) by team
# ==================================================
query4 = """
SELECT
    batting_team,
    match_id,
    innings,
    SUM(total_runs) AS total_runs,
    SUM(balls) AS powerplay_balls,
    ROUND((SUM(total_runs) / SUM(balls)) * 6, 2) AS powerplay_runrate
FROM
    processed_data
WHERE
    over < 6
GROUP BY
    batting_team, match_id, innings
ORDER BY
    match_id, innings
"""

powerplay_runrate = spark.sql(query4)

print("\n===== POWERPLAY RUN RATE BY TEAM =====")
powerplay_runrate.show(truncate=False)

# Stop the Spark session
spark.stop()
