from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("title", StringType())
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "fake_people") \
        .option("startingOffsets", "earliest") \
        .load()
    kafka_df.createOrReplaceTempView("fake_people")

    count_df = spark.sql("SELECT title, COUNT(1) count FROM fake_people GROUP BY 1 ORDER BY 2 DESC LIMIT 10")

    count_writer_query = count_df.writeStream \
        .format("json") \
        .queryName("Title Count Writer") \
        .outputMode("complete") \
        .option("path", "output") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    print("Listening to Kafka")
    invoice_writer_query.awaitTermination()

