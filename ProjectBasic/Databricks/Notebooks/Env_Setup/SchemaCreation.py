# Databricks notebook source
# MAGIC %md
# MAGIC ###Create a schema (database) in Databricks Unity Catalog

# COMMAND ----------

# Set the catalog (replace 'main' with your catalog name)
catalog_name = "databricks_lakehouse_demo_ws_e2"

# Schema (database) name
schema_name = "product_gold_db"

# PySpark command to create schema in Unity Catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA databricks_lakehouse_demo_ws_e2.product_silver_db;

# COMMAND ----------

# MAGIC %md
# MAGIC #### PySpark Code