# Databricks notebook source
import pyspark.sql.functions as F
from  pyspark.sql.functions import col, struct, to_json
from pyspark.sql.types import StructField, StructType, StringType, MapType

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/databricks/data.json

# COMMAND ----------

#create streaming dataframe
jsonSchema = StructType([ StructField("name", StringType(), True), 
                         StructField("occupation", StringType(), True) ])

inputPath = "dbfs:/FileStore/databricks/"
streamingInputDF = (
  spark
    .readStream 
    .option("multiline","true")                      
    .schema(jsonSchema)               # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .json(inputPath)
)
display(streamingInputDF)

# COMMAND ----------

display(streamingInputDF.selectExpr("name AS key", "to_json(struct(*)) AS value"))

# COMMAND ----------

#push streaming data into kafka topic
df= (streamingInputDF.selectExpr("name AS key", "to_json(struct(*)) AS value") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", ".....") \
  .option("topic", "topic_apo") \
  .option("kafka.security.protocol","SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("checkpointLocation", "dbfs:/FileStore/chekpoint/")
  .option("kafka.sasl.jaas.config", """kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="...." password="...";""") \
  .start()
)
