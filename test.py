from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

# Initialize SparkSession (you already have this)
spark = SparkSession.builder \
    .appName("KafkaStreamingApp") \
    .getOrCreate()

# Get the SparkContext from the SparkSession
sparkContext = spark.sparkContext

# Create the StreamingContext
batchDuration = 60  # 1-minute batches
ssc = StreamingContext(sparkContext, batchDuration)

# Example: Use the StreamingContext here
# ssc.start()

# To stop the Streaming job gracefully
ssc.stop()
