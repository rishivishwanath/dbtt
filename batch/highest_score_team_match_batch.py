from pyspark.sql import SparkSession
import logging

# Suppress specific warnings
logging.getLogger("org.apache.spark.sql.execution").setLevel(logging.ERROR)

# Create Spark Session
spark = SparkSession.builder \
    .appName("IPL_Highest_Scorers_Batch") \
    .config("spark.jars", "/home/hadoop/Downloads/mysql-connector-j_9.3.0-1ubuntu25.04_all/usr/share/java/mysql-connector-j-9.3.0.jar") \
    .getOrCreate()

# Set log level to reduce verbose output
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

# =====================================
# 1. Calculate basic player scoring stats - All player scores
# =====================================
query1 = """
SELECT 
    match_id,
    batting_team,
    innings,
    striker,
    SUM(CAST(runs_of_bat AS INT)) AS total_runs
FROM 
    runs_data
GROUP BY 
    match_id, batting_team, innings, striker
ORDER BY 
    match_id, batting_team, innings, total_runs DESC
"""

all_player_scores = spark.sql(query1)

print("\n===== ALL PLAYER SCORES =====")
all_player_scores.show(20, truncate=False)

# =====================================
# 2. Find top scorers by match/innings/team using SQL
# =====================================
query2 = """
WITH player_scores AS (
    SELECT 
        match_id,
        batting_team,
        innings,
        striker,
        SUM(CAST(runs_of_bat AS INT)) AS total_runs
    FROM 
        runs_data
    GROUP BY 
        match_id, batting_team, innings, striker
),
max_scores AS (
    SELECT 
        match_id,
        batting_team,
        innings,
        MAX(total_runs) AS max_runs
    FROM 
        player_scores
    GROUP BY 
        match_id, batting_team, innings
)
SELECT 
    ps.match_id,
    ps.batting_team,
    ps.innings,
    ps.striker,
    ps.total_runs
FROM 
    player_scores ps
JOIN 
    max_scores ms
ON 
    ps.match_id = ms.match_id AND
    ps.batting_team = ms.batting_team AND
    ps.innings = ms.innings AND
    ps.total_runs = ms.max_runs
ORDER BY 
    ps.match_id, ps.batting_team, ps.innings
"""

top_scorers = spark.sql(query2)

print("\n===== TOP SCORERS BY MATCH/INNINGS/TEAM =====")
top_scorers.show(50, truncate=False)

# =====================================
# 3. Overall highest run scorers across all matches
# =====================================
query3 = """
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

highest_overall_scorers = spark.sql(query3)

print("\n===== HIGHEST OVERALL SCORERS =====")
highest_overall_scorers.show(20, truncate=False)

# =====================================
# 4. Highest scorers by match
# =====================================
query4 = """
WITH player_match_scores AS (
    SELECT 
        match_id,
        striker,
        batting_team,
        SUM(CAST(runs_of_bat AS INT)) AS match_total
    FROM 
        runs_data
    GROUP BY 
        match_id, striker, batting_team
),
max_match_scores AS (
    SELECT 
        match_id,
        MAX(match_total) AS max_score
    FROM 
        player_match_scores
    GROUP BY 
        match_id
)
SELECT 
    pms.match_id,
    pms.striker,
    pms.batting_team,
    pms.match_total AS highest_score
FROM 
    player_match_scores pms
JOIN 
    max_match_scores mms
ON 
    pms.match_id = mms.match_id AND
    pms.match_total = mms.max_score
ORDER BY 
    pms.match_id
"""

highest_match_scorers = spark.sql(query4)

print("\n===== HIGHEST SCORERS BY MATCH =====")
highest_match_scorers.show(50, truncate=False)

# Stop the Spark session
spark.stop()
