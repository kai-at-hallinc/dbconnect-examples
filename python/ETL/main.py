# Databricks notebook source

# imports
from databricks.connect import DatabricksSession
import transformations as trans

# create Spark session:
spark = DatabricksSession.builder.profile("vscode").getOrCreate()

# Step 1 - Load the Bronze Tables
print("Step 1: ingesting data into bronze tables")

table_name_bronze = "main.default.taxi_demo_bronze"
df = trans.load_data(spark)
trans.save_data(df, table_name_bronze)

# Step 2 - Load the Silver Tables
print("Step 2: transforming data into silver tables")

df = spark.table(table_name_bronze)
df = trans.filter_columns(df)
df = trans.transform_columns(df)
df = trans.filter_invalid(df)

table_name_silver = "main.default.taxi_demo_silver"
trans.save_data(df, table_name_silver)

print(f"silver row count: {spark.table(table_name_silver).count()}")


# Step 3 Prepare the views
print("Step 3: creating gold layer views")

yellow_view = "main.default.taxi_demo_view_yellow"
green_view = "main.default.taxi_demo_view_green"

trans.create_view(spark, table_name_silver, yellow_view, 1)
trans.create_view(spark, table_name_silver, green_view, 2)


# query the views
spark.sql(f"SELECT * FROM {yellow_view}").show(5)
spark.sql(f"SELECT * FROM {green_view}").show(5)
