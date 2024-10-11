import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, count, window, date_format
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, IntegerType

class TestKafkaStreamingApp(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("KafkaStreamingAppTest") \
            .master("local[2]") \
            .getOrCreate()

        # Define the input schema
        cls.schema = StructType([
            StructField("view_id", StringType(), True),
            StructField("start_timestamp", TimestampType(), True),
            StructField("end_timestamp", TimestampType(), True),
            StructField("banner_id", LongType(), True),
            StructField("campaign_id", IntegerType(), True),
            StructField("network_id", IntegerType(), True)
        ])
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_spark_processing(self):
        # Create sample JSON input records (20 records)
        input_data = [
            "{\"view_id\":\"v1\", \"start_timestamp\":\"2023-10-08 12:00:00\", \"end_timestamp\":\"2023-10-08 12:01:00\", \"banner_id\":100, \"campaign_id\":1, \"network_id\":1}",
            "{\"view_id\":\"v2\", \"start_timestamp\":\"2023-10-08 12:00:00\", \"end_timestamp\":\"2023-10-08 12:00:40\", \"banner_id\":101, \"campaign_id\":1, \"network_id\":1}",
            "{\"view_id\":\"v1\", \"start_timestamp\":\"2023-10-08 12:01:00\", \"end_timestamp\":\"2023-10-08 12:01:30\", \"banner_id\":102, \"campaign_id\":2, \"network_id\":1}",
            "{\"view_id\":\"v1\", \"start_timestamp\":\"2023-10-08 12:01:00\", \"end_timestamp\":\"2023-10-08 12:01:30\", \"banner_id\":102, \"campaign_id\":2, \"network_id\":1}",
            # Add 17 more similar records for testing
        ]
        
        # Simulate reading from Kafka by creating DataFrame from JSON records
        df = self.spark.createDataFrame(input_data, StringType()).toDF("value")
        view_log_df = df.withColumn("parsed_value", from_json(col("value"), self.schema))

        # Extract fields and compute view duration
        valid_view_log_df = view_log_df.select(
            col("parsed_value.view_id"),
            col("parsed_value.start_timestamp"),
            col("parsed_value.end_timestamp"),
            col("parsed_value.banner_id"),
            col("parsed_value.campaign_id"),
            col("parsed_value.network_id")
        ).withColumn("view_duration", (col("end_timestamp").cast("long") - col("start_timestamp").cast("long")).cast("double"))

        # Simulate campaign.csv file (campaign_id, network_id)
        campaigns_data = [
            (1, "Campaign1", 1),
            (2, "Campaign2", 1)
        ]
        campaigns_columns = ["campaign_id", "campaign_name", "network_id"]
        campaigns_df = self.spark.createDataFrame(campaigns_data, campaigns_columns)

        # Rename the network_id in campaigns to avoid ambiguity
        campaigns_df = campaigns_df.withColumnRenamed("network_id", "campaign_network_id")

        # Perform join with campaigns.csv
        joined_df = valid_view_log_df.join(campaigns_df, "campaign_id", "inner")

        # Perform aggregation: average view duration and count per campaign_id, network_id per minute window
        aggregated_view_log_df = joined_df.groupBy(
            window(col("start_timestamp"), "1 minute"),
            col("network_id"),  # Use the network_id from view_log_df (original data)
            col("campaign_id")
        ).agg(
            avg("view_duration").alias("avg_duration"),
            count("view_id").alias("total_count")
        ).withColumn("minute_timestamp", date_format(col("window.start"), "yyyy-MM-dd-HH-mm")).drop("window")

        # Collect results and check if they match expected output
        results = aggregated_view_log_df.collect()
        print(results)

        # Expected aggregation checks
        self.assertEqual(len(results), 2)  # Ensure we have 2 output rows (2 different campaign_id)
        for result in results:
            if result["campaign_id"] == 1:
                self.assertEqual(result["total_count"], 2)  # Campaign 1 should have 2 records
                self.assertEqual(result["avg_duration"], 50.0)  # 60 seconds average for campaign 1
            elif result["campaign_id"] == 2:
                self.assertEqual(result["total_count"], 2)  # Campaign 2 should have 1 record
                self.assertEqual(result["avg_duration"], 30.0)  # 60 seconds for campaign 2

if __name__ == "__main__":
    unittest.main()
