// Databricks notebook source
val fileName = "adl://angandatalakestore.azuredatalakestore.net/data/customer-orders.csv"
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
val schema = StructType(
Array(
StructField("customerId",IntegerType,true),
StructField("orderId",IntegerType,true),
StructField("amount",DoubleType,true)
)
)
val data = spark.read.option("inferSchema",false).option("header","true").option("sep",",").schema(schema).csv(fileName)
data.schema
data.printSchema
data.createOrReplaceTempView("customerorders")
val result = spark.sql("SELECT customerId, Sum(amount) as TotalAmount FROM customerorders GROUP BY customerId")

result.sort(desc("TotalAmount")).show(10)


// COMMAND ----------

