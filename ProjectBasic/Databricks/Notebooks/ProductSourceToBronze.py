# Databricks notebook source
# MAGIC %run 
# MAGIC /Users/abhijitaagdscert2023@gmail.com/Env_Setup/Setup_Env

# COMMAND ----------

# dbutils.widgets.text("source_path", "dbfs:/mnt/s3productordereast2/categories/")
# dbutils.widgets.text("database_name", "databricks_lakehouse_demo_ws_e2.product_bronze_db")
# dbutils.widgets.text("table_name", "categories")

# COMMAND ----------

source_path = dbutils.widgets.get("source_path")
database_name = dbutils.widgets.get("database_name")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

ingest_json_to_delta(source_path, database_name, table_name)