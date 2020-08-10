// Databricks notebook source
// MAGIC %md ###Mount Data Lake Gen 2

// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "<application-id>",
  "fs.azure.account.oauth2.client.secret" -> "<key-name-for-service-credential>"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/<directory-id>/oauth2/token")

// Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/",
  mountPoint = "/mnt/<mount-name>",
  extraConfigs = configs)

// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "86da00b8-c93b-42e3-8f8f-48929d691355",
  "fs.azure.account.oauth2.client.secret" -> "v88Ptr32-1P.zSc-fUC1o1~271.G7UTop9",
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/e61513bf-36b0-411c-972e-aab3ec7612ff/oauth2/token")

// Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://mystaging101@mydatalake101.dfs.core.windows.net/",
  mountPoint = "/mnt/myStageDataLake",
  extraConfigs = configs)

dbutils.fs.mount(
  source = "abfss://mytarget101@mydatalake101.dfs.core.windows.net/",
  mountPoint = "/mnt/myTargetDataLake",
  extraConfigs = configs)

// COMMAND ----------

//dbutils.fs.unmount("/mnt/DatalakeGen2")

// COMMAND ----------

display(
  dbutils.fs.ls("dbfs:/mnt/myStageDataLake/stg_migADWorks/dbo.DimCustomer/")
)

// COMMAND ----------

display(
  dbutils.fs.ls("mnt/myTargetDataLake/")
)

// COMMAND ----------

//transforming DimCustomer dimension before loading to Synapse DWH
val df_customer = spark.read.parquet("dbfs:/mnt/myStageDataLake/stg_migADWorks/dbo.DimCustomer/")
df_customer.show(10, false)

// COMMAND ----------

// MAGIC %fs ls /mnt/myTargetDataLake/sales/

// COMMAND ----------

//below code to check role based access using another service principle
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", "d4b33fff-d5fe-4771-b885-9d46f88501e3")
spark.conf.set("fs.azure.account.oauth2.client.secret", "PwJdtR.7.bTF7Zok~uTL7Yj73J~GV68jQy")
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/e61513bf-36b0-411c-972e-aab3ec7612ff/oauth2/token")

// COMMAND ----------

display(dbutils.fs.ls("abfss://mystaging101@mydatalake101.dfs.core.windows.net/"))