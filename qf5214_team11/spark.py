import findspark
findspark.init('D:\spark\spark-2.4.3-bin-hadoop2.7')


from pyspark.sql import SparkSession

#spark = SparkSession.builder.getOrCreate()

from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, from_json
# from kafka import SimpleClient
# from kafka.common import OffsetRequestPayload
import logging

from pyspark.sql.functions import regexp_replace


# 导入 from_json 函数，该函数用于解析 JSON 数据
from pyspark.sql.functions import from_json, col
# 用于定义 JSON 数据的模式
from pyspark.sql.types import StructType, StructField, StringType, IntegerType



# Specify Kafka brokers addresses and topics
kafka_config = {'servers':['localhost:9092'], 'topics': ['market-data','news-data']}

schema_volume = types.StructType([
    types.StructField('Timestamp', types.StringType(),True),
    types.StructField('Open', types.FloatType(),True),
    types.StructField('Close', types.FloatType(),True),
    types.StructField('High', types.FloatType(),True),
    types.StructField('Low', types.FloatType(),True),
    types.StructField('Volume', types.IntegerType(),True),
    types.StructField('Volume(Dollar)', types.FloatType(),True),
    types.StructField('Last Price', types.FloatType(),True),
    types.StructField('Ticker', types.StringType(),True)
    ])

schema_news = types.StructType([
    types.StructField('publishedAt', types.StringType(),True),
    types.StructField('stock_queried', types.StringType(),True),
    types.StructField('source_id', types.StringType(),True),
    types.StructField('title', types.StringType(),True),
    types.StructField('description', types.StringType(),True),
    types.StructField('content', types.StringType(),True),
    ])


#schema = "publishedAt STRING, stock_queried STRING, source_id STRING, title STRING, description STRING, content STRING"



def build_schema(json_str):
    json_schema = spark.read.json(spark.sparkContext.parallelize([json_str])).schema
    return json_schema


spark = SparkSession.builder.appName("KafkaToSpark").getOrCreate()

# Construct a streaming DataFrame that reads from 'volume' topic
df_volume = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", 'localhost:9092') \
  .option("subscribe", 'market-data') \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .option("startingTimestamp", "2024-04-06T00:00:00.000Z") \
  .load() \
  .selectExpr("CAST(value AS STRING)") \
  .withColumn("value", regexp_replace("value", "\\\\", "")) \
  #.select(from_json(col("value").cast("string"), schema_news).alias("data")).select("data.*")

  #.select(F.from_json(F.col("value"), schema_news).alias("data")) \
  #.select("data.*") \
  # .withColumn("Timestamp_vol", F.to_timestamp(F.col("Timestamp"), "yyyy-MM-dd HH:mm:ss")) \
  # .drop("Timestamp")




query = df_volume.writeStream.outputMode("append").format("console").option("truncate", False).start()
query.awaitTermination()


# Round timestamps down to nearest 5 minutes
# df_volume = df_volume \
#   .withColumn("Timestamp_vol_floor", (F.floor(F.unix_timestamp("Timestamp_vol") / (5 * 60)) * 5 * 60).cast("timestamp"))

# Apply watermark
# df_volume = df_volume.withWatermark("Timestamp_vol", "5 minutes")
#
# # Calculate wick percentage
# df_volume = df_volume \
#     .withColumn("candle_size", F.col("2_high") - F.col("3_low")) \
#     .withColumn("wick_size", F.when(F.col("4_close") >= F.col("1_open"), (F.col("2_high") - F.col("4_close"))) \
#         .otherwise(F.col("3_low") - F.col("4_close"))) \
#     .withColumn("wick_prct", F.col("wick_size") / F.col("candle_size")) \
#     .drop("candle_size") \
#     .drop("wick_size")
