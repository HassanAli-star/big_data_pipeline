from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, IntegerType
from pyspark.sql.functions import from_json, window, avg, count, date_format, current_timestamp

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaStreamingApp") \
    .getOrCreate()

try:
    # Read from the Kafka topic as a streaming source
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "view_log") \
        .option("startingOffsets", "earliest") \
        .load()

    # Kafka returns key and value in binary format, so we cast it to string
    view_log_df = df.selectExpr("CAST(value AS STRING)")

    # Define schema for the JSON data
    schema = StructType([
        StructField("view_id", StringType(), True),
        StructField("start_timestamp", StringType(), True),  # JSON is in string format, will be cast later
        StructField("end_timestamp", StringType(), True),    # JSON is in string format, will be cast later
        StructField("banner_id", LongType(), True),
        StructField("campaign_id", IntegerType(), True),
        StructField("network_id", IntegerType(), True)
    ])

    # Parse the JSON in the 'value' column using the schema
    view_log_df = view_log_df.withColumn("parsed_value", from_json(col("value"), schema))

    # **Handle malformed JSON records**
    # Check if the 'parsed_value' column is null (indicating invalid JSON records)
    valid_view_log_df = view_log_df.filter(col("parsed_value").isNotNull())

    # Select the parsed columns
    valid_view_log_df = valid_view_log_df.withColumn("current_timestamp", current_timestamp()) \
        .select(
            col("parsed_value.view_id"),
            col("parsed_value.start_timestamp").cast(TimestampType()).alias("start_timestamp"),
            col("parsed_value.end_timestamp").cast(TimestampType()).alias("end_timestamp"),
            col("parsed_value.banner_id"),
            col("parsed_value.campaign_id"),
            col("parsed_value.network_id"),
            col("current_timestamp")
        ).withColumn("minute_timestamp", window(col("current_timestamp"), "1 minute")) \
         .withColumn("view_duration", (col("end_timestamp").cast("long") - col("start_timestamp").cast("long")).cast("double"))

    # Add watermark to handle late data
    valid_view_log_df = valid_view_log_df \
        .withWatermark("start_timestamp", "10 seconds") \
        .withColumnRenamed("network_id", "view_network_id")

    # Join with campaign data (simplified)
    campaigns_df = spark.read.option("header", "true").csv("/app/input_file/campaigns1.csv")

    # Join with campaign data
    valid_view_log_df = valid_view_log_df \
        .join(campaigns_df, "campaign_id", "inner") \
        .select(
            col("view_id"),
            col("start_timestamp"),
            col("end_timestamp"),
            col("banner_id"),
            col("campaign_id"),
            col("network_id"),
            col("current_timestamp"),
            col("minute_timestamp"),
            col("view_duration")
        )

    # Aggregating the data
    aggregated_view_log_df = valid_view_log_df \
        .groupBy(
            window(col("start_timestamp"), "1 minute"),
            col("network_id"),
            col("campaign_id")
        ) \
        .agg(
            avg("view_duration").alias("avg_duration"),
            count("view_id").alias("total_count")
        ) \
        .withColumn("minute_timestamp", date_format(col("window.start"), "yyyy-MM-dd-HH-mm")) \
        .drop("window")

    # **Create Partition Columns Separately**
    aggregated_view_log_df = aggregated_view_log_df \
        .withColumn("network_id_part", col("network_id")) \
        .withColumn("minute_timestamp_part", col("minute_timestamp"))

    # **Write the streaming data into Parquet format**
    query = aggregated_view_log_df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/app/tmp/") \
        .option("path", "/app/report/") \
        .partitionBy("network_id_part", "minute_timestamp_part") \
        .outputMode("append") \
        .trigger(processingTime='1 minute') \
        .start()

    query.awaitTermination()

except Exception as e:
    print(f"Error occurred: {e}")
    sys.exit(1)
