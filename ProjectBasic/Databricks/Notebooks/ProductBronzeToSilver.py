# Databricks notebook source
# MAGIC %run 
# MAGIC /Users/abhijitaagdscert2023@gmail.com/Env_Setup/Setup_Env

# COMMAND ----------

# dbutils.widgets.text("bronze_table", "databricks_lakehouse_demo_ws_e2.product_bronze_db.categories")
# dbutils.widgets.text("silver_table", "databricks_lakehouse_demo_ws_e2.product_silver_db.categories")
# dbutils.widgets.text("primary_key", "category_id")
# dbutils.widgets.text("selected_cols", "category_department_id,category_id,category_name")

# COMMAND ----------

bronze_table = dbutils.widgets.get("bronze_table")
silver_table = dbutils.widgets.get("silver_table")
primary_key = dbutils.widgets.get("primary_key")
selected_cols = dbutils.widgets.get("selected_cols")

# COMMAND ----------

load_bronze_to_silver(bronze_table, silver_table, primary_key, selected_cols)