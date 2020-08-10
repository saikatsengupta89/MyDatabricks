// Databricks notebook source
// MAGIC %md ###Section 1: Exploration Operations

// COMMAND ----------

// MAGIC %fs ls /mnt/myStageDataLake/sampledata/

// COMMAND ----------

// MAGIC %fs head /mnt/myStageDataLake/sampledata/YellowTaxiTripData.csv

// COMMAND ----------

val taxi_data_raw = spark.read
                     .option("header","true")
                     .csv("/mnt/myStageDataLake/sampledata/")
taxi_data_raw.createOrReplaceTempView("taxidataraw")
val taxi_data = spark.sql("select * from taxidataraw where VendorID is not null")
display(taxi_data)

// COMMAND ----------

// MAGIC %md ###Section 2: Analyse Data

// COMMAND ----------

taxi_data.printSchema

// COMMAND ----------

//get important fields out of the dataset
display(taxi_data.describe("passenger_count", "trip_distance"))

// COMMAND ----------

// MAGIC %md ###Section 3: Clean Data

// COMMAND ----------

import org.apache.spark.sql.functions.col
//some more cleaning on the dataset
println("Before filter = " + taxi_data.count())

var taxi_data_filtered = taxi_data.where("passenger_count > 0")
                                  .filter(col("trip_distance") > 0)
//after filter, total count
println("After filter = "+ taxi_data_filtered.count())

// COMMAND ----------

// Display the count before filtering
println("Before filter = " + taxi_data_filtered.count())

// Drop rows with nulls in PULocationID or DOLocationID
// dataframe has a function na which intakes a sequence of columns
taxi_data_filtered = taxi_data_filtered
                          .na.drop(
                                    Seq("PULocationID", "DOLocationID")
                                  )

// Display the count after filtering
println("After filter = " + taxi_data_filtered.count())

// COMMAND ----------

// MAGIC %md ###Section 4: Transformation

// COMMAND ----------

taxi_data_filtered = taxi_data_filtered                                                
                        .withColumnRenamed("PUlocationID", "PickupLocationId")
                        .withColumnRenamed("DOlocationID", "DropLocationId")                        
taxi_data_filtered.printSchema

// COMMAND ----------

// MAGIC %md ###Section 5: Loading Data

// COMMAND ----------

taxi_data_filtered  
    .write
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
    .mode(SaveMode.Overwrite)
    .csv("/mnt/myStageDataLake/sampledata/ProcessedTaxiData/YellowTaxiData.csv")

// COMMAND ----------

taxi_data_filtered  
    .write
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
    .mode(SaveMode.Overwrite)
    .parquet("/mnt/myStageDataLake/sampledata/ProcessedTaxiData/YellowTaxiData.parquet")