# Databricks notebook source
# MAGIC %md ######MY Note Book Does three things
# MAGIC 1. Reads data from an WEB API
# MAGIC 2. Does some wrangling to it using the Apache Spark python API and
# MAGIC 3. Write back the final form of the data back to an Azure Blob Storage as a Delta format in CSV file

# COMMAND ----------

import json
import requests
#connect to azure notebook using key-vault
blobAccessKeyVault = dbutils.secrets.get(scope="mySecretScopeBlobKV", key="accessKey")
storageAccountName = "mydatalake101"
containerName      = "mystaging101"

spark.conf.set (
  "fs.azure.account.key."+ storageAccountName + ".dfs.core.windows.net", blobAccessKeyVault
)

# COMMAND ----------

#web api url
#"https://jsonplaceholder.typicode.com/todos"
url = "https://api.covid19india.org/raw_data8.json"
#CallWebApi
myResponse = requests.get(url)

# COMMAND ----------

from pyspark.sql import types as types
schema_def= types.StructType([
  types.StructField("agebracket", types.StringType()),
  types.StructField("contractedfromwhichpatientsuspected", types.StringType()),
  types.StructField("currentstatus", types.StringType()),
  types.StructField("dateannounced", types.StringType()),
  types.StructField("detectedcity", types.StringType()),
  types.StructField("detecteddistrict", types.StringType()),
  types.StructField("detectedstate", types.StringType()),
  types.StructField("entryid", types.StringType()),
  types.StructField("gender", types.StringType()),
  types.StructField("nationality", types.StringType()),
  types.StructField("notes", types.StringType()),
  types.StructField("numcases", types.StringType()),
  types.StructField("patientnumber", types.StringType()),
  types.StructField("source1", types.StringType()),
  types.StructField("source2", types.StringType()),
  types.StructField("source3", types.StringType()),
  types.StructField("statecode", types.StringType()),
  types.StructField("statepatientnumber", types.StringType()),
  types.StructField("statuschangedate", types.StringType()),
  types.StructField("typeoftransmission", types.StringType())
]) 
if (myResponse.ok):
  jData = json.loads(myResponse.content)
  raw_data = jData['raw_data']
  spark_dataframe = spark.createDataFrame(raw_data, schema_def)
else:
  myResponse.raise_for_status()
display(spark_dataframe)

# COMMAND ----------

#writing the covid dataset into blob storage
savePath = "abfss://"+containerName+"@"+storageAccountName+".dfs.core.windows.net/sampledata/covid_dataset/"
spark_dataframe.repartition(1)\
               .write\
               .format("csv")\
               .mode("overwrite")\
               .save(savePath)