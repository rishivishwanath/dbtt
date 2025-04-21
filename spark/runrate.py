from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, expr, round
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("IPL_RunRate_Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema definition including 'extras'
schema = StructType() \
    .add("match_id", StringType()) \
    .add("batting_team", StringType()) \
    .add("innings", StringType()) \
    .add("over", StringType()) \
    .add("striker", StringType()) \
    .add("runs_of_bat", StringType()) \
    .add("extras", StringType())

# Read Kafka stream from 'rate' topic
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rate") \
    .option("startingOffsets", "latest") \
    .load()

# Parse and cast data
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("innings", col("innings").cast("int")) \
    .withColumn("over", col("over").cast("float")) \
    .withColumn("runs_of_bat", col("runs_of_bat").cast("int")) \
    .withColumn("extras", col("extras").cast("int")) \
    .withColumn("total_runs", col("runs_of_bat") + col("extras")) \
    .withColumn("balls", expr("1"))

# ==================================================
# 1. Run rate grouped by match_id, innings, team
# ==================================================
runrate_by_match = df_parsed.groupBy("match_id", "innings", "batting_team") \
    .agg(
        _sum("total_runs").alias("total_runs"),
        _sum("balls").alias("total_balls")
    ) \
    .withColumn("run_rate", round((col("total_runs") / col("total_balls")) * 6, 2)) \
    .orderBy("match_id", "innings")

# ==================================================
# 2. Overall run rate of each team (across matches)
# ==================================================
overall_team_runrate = df_parsed.groupBy("batting_team") \
    .agg(
        _sum("total_runs").alias("total_runs"),
        _sum("balls").alias("total_balls")
    ) \
    .withColumn("overall_run_rate", round((col("total_runs") / col("total_balls")) * 6, 2)) \
    .orderBy(col("overall_run_rate").desc())

# ==================================================
# 3. Run rate per striker (player)
# ==================================================
striker_runrate = df_parsed.groupBy("striker") \
    .agg(
        _sum("total_runs").alias("total_runs"),
        _sum("balls").alias("balls_faced")
    ) \
    .withColumn("run_rate", round((col("total_runs") / col("balls_faced")) * 6, 2)) \
    .orderBy(col("run_rate").desc())

# ==================================================
# 4. Run rate in Powerplay (overs < 6) by team
# ==================================================
powerplay_runrate = df_parsed.filter(col("over") < 6) \
    .groupBy("batting_team", "match_id", "innings") \
    .agg(
        _sum("total_runs").alias("total_runs"),
        _sum("balls").alias("powerplay_balls")
    ) \
    .withColumn("powerplay_runrate", round((col("total_runs") / col("powerplay_balls")) * 6, 2)) \
    .orderBy("match_id", "innings")

# ==================================================
# Output Queries to Console
# ==================================================
query1 = runrate_by_match.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Run Rate by Match, Innings, Team") \
    .trigger(processingTime='5 seconds') \
    .start()

query2 = overall_team_runrate.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Overall Team Run Rate") \
    .start()

query3 = striker_runrate.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Run Rate per Striker") \
    .trigger(processingTime='5 seconds') \
    .start()

query4 = powerplay_runrate.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Powerplay Run Rate by Team") \
    .start()

# Await termination
spark.streams.awaitAnyTermination()

