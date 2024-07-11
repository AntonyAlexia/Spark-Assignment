# Databricks notebook source
csv_df = spark.read.csv("dbfs:/FileStore/all_stats_t_series.csv", header=True, inferSchema=True)
display(csv_df)


# COMMAND ----------

json_df = spark.read.json("dbfs:/FileStore/sample_data.json")
display(json_df)

# COMMAND ----------


parquet_df = spark.read.parquet("dbfs:/FileStore/userdata4.parquet")
display(parquet_df)


# COMMAND ----------

avro_df = spark.read.format("avro").load("dbfs:/FileStore/twitter.avro")

display(avro_df)


# COMMAND ----------

from pyspark.sql import functions as F

df1 = spark.createDataFrame([(1, "Alexia"), (2, "Sebasthikannu")],["id","name"])
df2 = spark.createDataFrame([(1, "Data analyst"), (2, "Engineering")],["id","name"])
broadcast_df2 = F.broadcast(df2)
joined_df = df1.join(broadcast_df2, "id")
display(joined_df)


# COMMAND ----------

filtered_df = df1.filter(df1.name == "Alexia")

display(filtered_df)


# COMMAND ----------

df = spark.createDataFrame([(1, 100), (2, 200), (3, 300)], ["id", "value"])
agg_df = df.agg(F.max("value").alias("max_value"), F.min("value").alias("min_value"), F.avg("value").alias("avg_value"))
display(agg_df)


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType
schema = StructType([
    StructField("firstName", StringType(), True),
    StructField("lastName", StringType(), True),
    StructField("phoneNumbers", LongType(), True)
])

typed_json_df = spark.read.schema(schema).json("dbfs:/FileStore/sample_data.json")
display(typed_json_df)


# COMMAND ----------

df = spark.range(0, 20)
df_repartitioned = df.repartition(10)
df_coalesced = df.coalesce(5)
print("Number of partitions after repartition:", df_repartitioned.rdd.getNumPartitions())
print("Number of partitions after coalesce:", df_coalesced.rdd.getNumPartitions())


# COMMAND ----------

renamed_df = df.withColumnRenamed("value", "new_value")
display(renamed_df)


# COMMAND ----------


dtf = df.withColumn("new_column", F.lit("new_value"))

display(dtf)


# COMMAND ----------

from pyspark.sql import Row
json_data = [
    """{ "name" : "antony alexia", "dob" : "01-07-2002" }""",
    """{ "name" : "sebasthikannu", "dob" : "06-02-1972", "phone" : 1234567890 }"""
]
rdd = spark.sparkContext.parallelize(json_data)
json_df = spark.read.json(rdd)
new_struct_df = json_df.select(
    F.struct(F.col("name"), F.col("dob"), F.col("phone")).alias("personal_data")
)
display(new_struct_df)
new_struct_df.write.json("/data/output_json")


# COMMAND ----------

from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaReader")

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaReader") \
    .getOrCreate()

# Kafka configurations
kafka_bootstrap_servers = "localhost:9092"  # Replace with your actual Kafka bootstrap servers
kafka_topic = "test_topic"  # Replace with your actual Kafka topic name

logger.info("Reading from Kafka...")
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()
logger.info("Selecting and casting Kafka value to string...")
kafka_value_df = kafka_df.selectExpr("CAST(value AS STRING)")
logger.info("Kafka DataFrame schema:")
kafka_value_df.printSchema()
def log_message(df, epoch_id):
    logger.info(f"Processing batch {epoch_id}")
    df.show(truncate=False)
logger.info("Starting streaming query to print to console...")
query = kafka_value_df.writeStream \
    .outputMode("append") \
    .foreachBatch(log_message) \
    .start()
query.awaitTermination()



# COMMAND ----------

# MAGIC %sh
# MAGIC pip install lib.logger

# COMMAND ----------

from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("file streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    logger = Log4j(spark)

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoices") \
        .option("startingOffsets", 'earliest')\
        .load()

    kafka_df.printSchema()      

