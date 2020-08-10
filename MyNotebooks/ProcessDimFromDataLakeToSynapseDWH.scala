// Databricks notebook source
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

val NAME_POLYBASE_STG = "temp_polybase"
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

// read table from Synapse SQLDW
val df: DataFrame = spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", JDBC_URL)
  .option("tempDir", TEMP_DIR)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "dbo.DimCurrency")
  .load()
//df.show()

// COMMAND ----------

// MAGIC %md ##### load DimCustomer to Synapse DWH

// COMMAND ----------

val df_customer_stg = spark.read.parquet("/mnt/myStageDataLake/stg_migADWorks/dbo.DimCustomer/")
df_customer_stg.createOrReplaceTempView("dim_customer")

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table if exists dim_customer_tgt;
// MAGIC create table dim_customer_tgt as 
// MAGIC select 
// MAGIC CustomerKey,
// MAGIC GeographyKey,
// MAGIC CustomerAlternateKey,
// MAGIC case when MaritalStatus ='S' and Gender='M' then 'Master'
// MAGIC      when MaritalStatus ='S' and Gender='F' then 'Miss'
// MAGIC      when MaritalStatus ='M' and Gender='M' then 'Mr'
// MAGIC      when MaritalStatus ='M' and Gender='F' then 'Mrs'
// MAGIC      else null end as Title,
// MAGIC FirstName,
// MAGIC MiddleName,
// MAGIC LastName,
// MAGIC concat(FirstName,' ',MiddleName,' ',LastName) as FullName,
// MAGIC BirthDate,
// MAGIC MaritalStatus,
// MAGIC Gender,
// MAGIC EmailAddress,
// MAGIC YearlyIncome,
// MAGIC case when YearlyIncome between 200000 and 300000 then 'High Salary'
// MAGIC      when YearlyIncome between 100000 and 200000 then 'Medium Salary'
// MAGIC      else 'Low Salary'
// MAGIC      end as SalaryBucket,
// MAGIC TotalChildren,
// MAGIC NumberChildrenAtHome,
// MAGIC EnglishEducation,
// MAGIC EnglishOccupation,
// MAGIC HouseOwnerFlag,
// MAGIC NumberCarsOwned,
// MAGIC AddressLine1,
// MAGIC AddressLine2,
// MAGIC Phone,
// MAGIC DateFirstPurchase,
// MAGIC CommuteDistance
// MAGIC from dim_customer;

// COMMAND ----------

/*
customer table schema created in SQLDW
create table dbo.DimCustomer (
CustomerKey int,
GeographyKey int,
CustomerAlternateKey varchar(50),
Title varchar(500),
FirstName varchar(500),
MiddleName varchar(500),
LastName varchar(500),
FullName varchar(500),
BirthDate date,
MaritalStatus varchar(10),
Gender varchar(10),
EmailAddress varchar(500),
YearlyIncome decimal(19,4),
SalaryBucket varchar(50),
TotalChildren int,
NumberChildrenAtHome int,
EnglishEducation varchar(50),
EnglishOccupation varchar(50),
HouseOwnerFlag varchar(50),
NumberCarsOwned int,
AddressLine1 varchar(500),
AddressLine2 varchar(500),
Phone varchar(50),
DateFirstPurchase date,
CommuteDistance varchar(50)
);
*/
val df_customer_tgt= spark.sql("select * from dim_customer_tgt")
df_customer_tgt.write
               .format("com.databricks.spark.sqldw")
               .mode(SaveMode.Overwrite)
               .option("url", JDBC_URL)
               .option("forwardSparkAzureStorageCredentials", "true")
               .option("dbTable", "dbo.DimCustomer")
               .option("tempDir", TEMP_DIR)
               .save()

// COMMAND ----------

// MAGIC %md ##### load DimEmployee to Synapse DWH

// COMMAND ----------

val df_employee_stg = spark.read.parquet("/mnt/myStageDataLake/stg_migADWorks/dbo.DimEmployee/")
df_employee_stg.createOrReplaceTempView("dim_employee")

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table if exists dim_employee_tgt;
// MAGIC create table dim_employee_tgt as
// MAGIC select 
// MAGIC EmployeeKey,
// MAGIC ParentEmployeeKey,
// MAGIC EmployeeNationalIDAlternateKey,
// MAGIC SalesTerritoryKey,
// MAGIC FirstName,
// MAGIC LastName,
// MAGIC MiddleName,
// MAGIC concat(FirstName,' ',MiddleName,' ',LastName) as FullName,
// MAGIC Title,
// MAGIC HireDate,
// MAGIC BirthDate,
// MAGIC LoginID,
// MAGIC EmailAddress,
// MAGIC Phone,
// MAGIC MaritalStatus,
// MAGIC EmergencyContactName,
// MAGIC EmergencyContactPhone,
// MAGIC SalariedFlag,
// MAGIC Gender,
// MAGIC PayFrequency,
// MAGIC BaseRate,
// MAGIC VacationHours,
// MAGIC SickLeaveHours,
// MAGIC CurrentFlag,
// MAGIC SalesPersonFlag,
// MAGIC DepartmentName,
// MAGIC StartDate,
// MAGIC EndDate,
// MAGIC Status
// MAGIC from dim_employee;

// COMMAND ----------

/*
create table dbo.DimEmployee (
EmployeeKey int,
ParentEmployeeKey int,
EmployeeNationalIDAlternateKey varchar(100),
SalesTerritoryKey int,
FirstName varchar(100),
LastName varchar(100),
MiddleName varchar(100),
FullName varchar(500),
Title varchar(100),
HireDate date,
BirthDate date,
LoginID varchar(100),
EmailAddress varchar(200),
Phone varchar(100),
MaritalStatus varchar(50),
EmergencyContactName varchar(50),
EmergencyContactPhone varchar(50),
SalariedFlag char(5),
Gender varchar(50),
PayFrequency int,
BaseRate decimal(19,4),
VacationHours int,
SickLeaveHours int,
CurrentFlag char(5),
SalesPersonFlag char(5),
DepartmentName varchar(100),
StartDate date,
EndDate date,
Status varchar(50)
)
*/
val df_employee_tgt= spark.sql("select * from dim_employee_tgt")
df_employee_tgt.write
               .format("com.databricks.spark.sqldw")
               .mode(SaveMode.Overwrite)
               .option("url", JDBC_URL)
               .option("forwardSparkAzureStorageCredentials", "true")
               .option("dbTable", "dbo.DimEmployee")
               .option("tempDir", TEMP_DIR)
               .save()

// COMMAND ----------

// MAGIC %md ##### load DimAccount to Synapse DWH

// COMMAND ----------

val df_account_stg = spark.read.parquet("/mnt/myStageDataLake/stg_migADWorks/dbo.DimAccount/")
df_account_stg.createOrReplaceTempView("dim_account")

val df_account_tgt= spark.sql (
"select "+
"AccountKey, "+
"ParentAccountKey, "+
"AccountCodeAlternateKey, "+
"ParentAccountCodeAlternateKey, "+
"AccountDescription, "+
"AccountType, "+
"ValueType "+
"from dim_account"
)

df_account_tgt.write
               .format("com.databricks.spark.sqldw")
               .mode(SaveMode.Overwrite)
               .option("url", JDBC_URL)
               .option("forwardSparkAzureStorageCredentials", "true")
               .option("dbTable", "dbo.DimAccount")
               .option("tempDir", TEMP_DIR)
               .save()

// COMMAND ----------

// MAGIC %md ##### load DimProduct to Synapse DWH

// COMMAND ----------

val df_product_stg = spark.read.parquet("/mnt/myStageDataLake/stg_migADWorks/dbo.DimProduct/")
df_product_stg.createOrReplaceTempView("dim_product")

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table if exists dim_product_tgt;
// MAGIC create table dim_product_tgt as
// MAGIC select
// MAGIC ProductKey,
// MAGIC ProductAlternateKey,
// MAGIC ProductSubcategoryKey,
// MAGIC EnglishProductName,
// MAGIC nvl(StandardCost,0) StandardCost,
// MAGIC FinishedGoodsFlag,
// MAGIC SafetyStockLevel,
// MAGIC ReorderPoint,
// MAGIC DaysToManufacture,
// MAGIC nvl(DealerPrice,0) DealerPrice,
// MAGIC Class,
// MAGIC EnglishDescription,
// MAGIC to_date(StartDate) StartDate,
// MAGIC to_date(EndDate) EndDate,
// MAGIC Status
// MAGIC from dim_product;

// COMMAND ----------

/*
create table dbo.DimProduct 
(
ProductKey int,
ProductAlternateKey varchar(100),
ProductSubcategoryKey int,
EnglishProductName varchar(200),
StandardCost int,
FinishedGoodsFlag varchar(100),
SafetyStockLevel int,
ReorderPoint int,
DaysToManufacture int,
DealerPrice decimal,
Class varchar(10),
EnglishDescription varchar(100),
StartDate datetime,
EndDate datetime,
Status varchar(100)
);
*/
val df_product_tgt= spark.sql("select * from dim_product_tgt")
df_product_tgt.write
              .format("com.databricks.spark.sqldw")
              .mode(SaveMode.Overwrite)
              .option("url", JDBC_URL)
              .option("forwardSparkAzureStorageCredentials", "true")
              .option("dbTable", "dbo.DimProduct")
              .option("tempDir", TEMP_DIR)
              .save()