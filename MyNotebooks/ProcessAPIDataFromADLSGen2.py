# Databricks notebook source
blobAccessKey      = dbutils.secrets.get(scope="mySecretScopeBlob", key="accessKey")
blobAccessKeyVault = dbutils.secrets.get(scope="mySecretScopeBlobKV", key="accessKey")
storageAccountName = "mydatalake101"
containerName      = "mystaging101"

spark.conf.set (
  "fs.azure.account.key."+ storageAccountName + ".dfs.core.windows.net", blobAccessKeyVault
)

# COMMAND ----------

rawData= spark.read.option("multiline","true").json("abfss://mystaging101@mydatalake101.dfs.core.windows.net/dumpCovidData/")
rawData.registerTempTable("rawData")

# COMMAND ----------

rawDataExplode= spark.sql("select explode(raw_data) as raw_data from rawData")
rawDataExplode.registerTempTable("rawDataExplode")
#rawDataExplode.show(10)
rawDataExplode.printSchema()

covidData= spark.sql("select "+
                     "nvl(cast(raw_data.agebracket as int), -1) agebracket, "+
                     "raw_data.contractedfromwhichpatientsuspected as patientsuspected, "+
                     "raw_data.currentstatus, "+
                     "from_unixtime(unix_timestamp(raw_data.dateannounced,'MM/dd/yyyy'), 'yyyyMMdd') timekey, "+
                     "raw_data.detectedcity, "+
                     "nvl(raw_data.detecteddistrict, 'unassigned') detecteddistrict, "+
                     "raw_data.detectedstate, "+
                     "nvl(raw_data.gender, 'M') gender, "+
                     "raw_data.nationality, "+
                     "raw_data.notes, "+
                     "cast(raw_data.numcases as int) numcases, "+
                     "raw_data.patientnumber, "+
                     "raw_data.source1, "+
                     "raw_data.source2, "+
                     "raw_data.source3, "+
                     "raw_data.statecode, "+
                     "raw_data.statepatientnumber, "+
                     "raw_data.statuschangedate, "+
                     "raw_data.typeoftransmission "+
                     "from rawDataExplode")
display(covidData)

# COMMAND ----------

covidData.repartition(5) \
         .write \
         .partitionBy("timekey") \
         .mode("overwrite") \
         .format("parquet") \
         .option("compression","snappy")\
         .save("abfss://mytarget101@mydatalake101.dfs.core.windows.net/covidData")