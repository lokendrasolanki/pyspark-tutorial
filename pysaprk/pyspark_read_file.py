# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create a PySpark session
spark = SparkSession.builder.master("local[1]").appName("csv_reader").getOrCreate()


df = (spark
      .read
      .format("csv") # file format like csv, avro, orc, parquet
      .option("header", True) # For header
      .option("sep",",")    # use for separator 
      .option("inferSchema", True) # for schema true for spark internal schema 
      .load("dbfs:/FileStore/tables/person.csv") # give the path of the file 
      )

df.display()


df.printSchema() # printing the schema

# Add external Schema
schema =  StructType([StructField('PersonID', IntegerType(), True), 
                      StructField('Name', StringType(), True),
                      StructField('Email', StringType(), True), 
                      StructField('Score', IntegerType(), True)])


df = (spark
      .read
      .format("csv") 
      .schema(schema) # give our external schema
      .option("header", True) 
      .option("sep",",")    
      .load("dbfs:/FileStore/tables/person.csv")
      )

df.display()




