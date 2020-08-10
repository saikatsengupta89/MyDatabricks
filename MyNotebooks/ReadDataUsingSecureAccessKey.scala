// Databricks notebook source
val blobAccessKey      = dbutils.secrets.get(scope="mySecretScopeBlob", key="accessKey")
val blobAccessKeyVault = dbutils.secrets.get(scope="mySecretScopeBlobKV", key="accessKey")
val storageAccountName = "mydatalake101"
val containerName      = "mytarget101"

spark.conf.set (
  "fs.azure.account.key."+ storageAccountName + ".dfs.core.windows.net", blobAccessKeyVault
)

// COMMAND ----------

val fileName = "dim_customer.parquet"
val CustomerData = spark.read.parquet("abfss://mytarget101@mydatalake101.dfs.core.windows.net/dimensions/" + fileName)
display(CustomerData)