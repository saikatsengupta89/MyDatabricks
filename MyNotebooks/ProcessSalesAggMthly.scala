// Databricks notebook source
val refresh_month = dbutils.widgets.get("month_key")

// COMMAND ----------

val salesData = spark.read.parquet(s"/mnt/myTargetDataLake/facts/fact_internet_sales/fact_internet_sales_$refresh_month.parquet")
val customerData = spark.read.parquet("/mnt/myTargetDataLake/dimensions/dim_customer.parquet")
salesData.createOrReplaceTempView("fact_internet_sales")
customerData.createOrReplaceTempView("dim_customer")
//customerData.printSchema()
//salesData.printSchema()

// COMMAND ----------

//doing all the trasnformation using spark sql
val transformed_data = spark.sql (s""" 
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
from fact_internet_sales fct
inner join dim_customer cus on cus.CustomerKey= fct.CustomerKey
where round(YearlyIncome, 0) >=8000
and case when cus.Gender in ('M','F') then 1 
         else 0 end =1
group by
substr(OrderDateKey, 1, 6),
cus.FirstName,
cus.LastName,
concat(cus.FirstName, ' ', cus.MiddleName, ' ', cus.LastName),
cus.EmailAddress,
cus.Gender,
cus.MaritalStatus
""")

// COMMAND ----------

//path check before writing
val base_path ="/mnt/myTargetDataLake/facts/fact_cust_sales_agg/"
try {
  dbutils.fs.ls(base_path)
}
catch {
  case e: Exception => "Base Path doesn't exists"
}

// COMMAND ----------

//writing the data into the specific aggregate folder in the data lake
transformed_data.repartition(1)
                .write
                .mode(SaveMode.Overwrite)
                .parquet(base_path.concat(s"fact_cust_sales_agg_$refresh_month"))

// COMMAND ----------

//passing a message to data factory
val message = s"Sales monthly aggregate completed for month key: $refresh_month"
dbutils.notebook.exit(message)