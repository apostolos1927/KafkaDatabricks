# Databricks notebook source
import pyspark.sql.functions as F
from  pyspark.sql.functions import col, struct, to_json
from pyspark.sql.types import StructField, StructType, StringType, MapType

# COMMAND ----------

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "....") \
      .option("subscribe", "topic_apo") \
      .option("startingOffsets", "earliest") \
      .option("kafka.security.protocol","SASL_SSL") \
      .option("kafka.sasl.mechanism", "PLAIN") \
      .option("kafka.sasl.jaas.config", """kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="...." password="...";""") \
    .load()


# COMMAND ----------

json_schema = StructType(
    [   StructField("name", StringType(), True),
        StructField("occupation", StringType(), True)
    ]
)

# COMMAND ----------

df3 = df.withColumn('value', F.from_json(F.col('value').cast('string'), json_schema))  \
      .select(F.col("value.name"),F.col("value.occupation")) 
display(df3)
