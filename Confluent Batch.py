# Databricks notebook source
import pyspark.sql.functions as F
from  pyspark.sql.functions import col, struct, to_json
from pyspark.sql.types import StructField, StructType, StringType, MapType

# COMMAND ----------

dataDictionary = [
        ('James','driver'),
        ('Michael','teacher'),
        ('Robert','engineer'),
        ('Washington','architect'),
        ('Jefferson','CEO')
        ]
df = spark.createDataFrame(data=dataDictionary, schema = ["name","occupation"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

#display input data
df2 = df.selectExpr("name AS key", "to_json(struct(*)) AS value")
display(df2)

# COMMAND ----------

#write to topic
(df.selectExpr("name AS key", "to_json(struct(*)) AS value") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", ".....") \
  .option("topic", "topic_apo") \
  .option("kafka.security.protocol","SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.sasl.jaas.config", """kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="...." password="....";""") \
  .save()
)

# COMMAND ----------

#read from topic
dfread = spark \
      .read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "....") \
      .option("subscribe", "topic_apo") \
      .option("startingOffsets", "earliest") \
      .option("endingOffsets", "latest")  \
      .option("kafka.security.protocol","SASL_SSL") \
      .option("kafka.sasl.mechanism", "PLAIN") \
      .option("kafka.sasl.jaas.config", """kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="...." password="...";""") \
      .load()
display(dfread)

# COMMAND ----------

json_schema = StructType(
    [   StructField("name", StringType(), True),
        StructField("occupation", StringType(), True)
    ]
)

# COMMAND ----------

#display data from topic
df3 = dfread.withColumn('value', F.from_json(F.col('value').cast('string'), json_schema))  \
      .select(F.col("value.name"),F.col("value.occupation")) 
display(df3)
