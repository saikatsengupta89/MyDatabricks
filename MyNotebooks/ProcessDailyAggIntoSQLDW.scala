// Databricks notebook source
//dbutils.widgets.text("MONTH_KEY","202007")
val refresh_month = dbutils.widgets.get("MONTH_KEY")

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

val NAME_POLYBASE_STG ="temp_polybase"
val NAME_STORAGE      = "mydatalake101"
val NAME_CONTAINER    = "mystaging101"
val DATABASE_USER     = "admin101"
val DATABASE_PASS     = "Simple@1616"
val STORAGE_ACCESS_KEY= "SmdZkaSOgPOcyvZy7Gga3PaOuH3osm6/PdXWjWwR9+T163tG3ZoTihvIGsaIvHdvapbN0MVqTeO/lIAdlPenEw=="
// Set up the Blob storage account access key in the notebook session conf.
spark.conf.set(s"fs.azure.account.key.$NAME_STORAGE.blob.core.windows.net", STORAGE_ACCESS_KEY)

val JDBC_URL= s"jdbc:sqlserver://mysqldwserver101.database.windows.net:1433;database=SQLDWH101;user=$DATABASE_USER@mysqldwserver101;password=$DATABASE_PASS;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

val TEMP_DIR = s"wasbs://$NAME_CONTAINER@$NAME_STORAGE.blob.core.windows.net/$NAME_POLYBASE_STG"

// COMMAND ----------

// read DimCustomer from Synapse SQLDW
val dimCustomer =  spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", JDBC_URL)
  .option("tempDir", TEMP_DIR)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "dbo.DimCustomer")
  .load()
dimCustomer.createOrReplaceTempView("DimCustomer")
//display(dimCustomer)

// COMMAND ----------

val factInternetSales = spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", JDBC_URL)
  .option("tempDir", TEMP_DIR)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("query", s"select * from dbo.FactInternetSales where SUBSTRING(cast(OrderDateKey as varchar),1,6)=$refresh_month")
  .load()
factInternetSales.createOrReplaceTempView("FactInternetSales")

// COMMAND ----------

/*
create this table in SQLDW to insert data from below transformation

create table dbo.FactInternetSalesAgg (
MonthKey varchar(300),
FirstName varchar(300),
LastName varchar(300),
FullName varchar(300),
EmailAddress varchar(300),
Gender varchar(300),
MaritalStatus varchar(300),
TotalSales decimal
)
*/

val transformedData = spark.sql (s""" 
select 
substr(OrderDateKey, 1, 6) month_key,
cus.FirstName as first_name,
cus.LastName as last_name,
concat(cus.FirstName, ' ', cus.MiddleName, ' ', cus.LastName) as full_name,
cus.EmailAddress as email_address,
case when upper(cus.Gender)= 'M' then 'Male'
     when upper(cus.Gender)= 'F' then 'Female'
	 else 'Undefined'
     end gender,
case when cus.MaritalStatus='S' then 'Single'
     when cus.MaritalStatus='M' then 'Married'
	 else 'Undefined'
	 end as marital_status,
sum(fct.SalesAmount) total_sales
from FactInternetSales fct
inner join DimCustomer cus on cus.CustomerKey= fct.CustomerKey
group by
substr(OrderDateKey, 1, 6),
cus.FirstName,
cus.LastName,
concat(cus.FirstName, ' ', cus.MiddleName, ' ', cus.LastName),
cus.EmailAddress,
cus.Gender,
cus.MaritalStatus
""")

transformedData.write
               .format("com.databricks.spark.sqldw")
               .mode(SaveMode.Append)
               .option("url", JDBC_URL)
               .option("forwardSparkAzureStorageCredentials", "true")
               .option("dbTable", "dbo.FactInternetSalesAgg")
               .option("tempDir", TEMP_DIR)
               .save()