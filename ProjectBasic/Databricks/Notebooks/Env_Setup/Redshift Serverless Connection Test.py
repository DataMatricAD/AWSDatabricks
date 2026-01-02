# Databricks notebook source
# MAGIC %md
# MAGIC #### Test Network Connection Between Redshift Serverless and Databricks

# COMMAND ----------

import socket

host = "databricks-demo-wg.********.us-east-1.redshift-serverless.amazonaws.com"
port = 5439

try:
    socket.create_connection((host, port), timeout=10)
    print("✅ Databricks in us-east-2 can reach Redshift in us-east-1!")
except Exception as e:
    print(f"❌ Databricks cannot connect to Redshift: {e}")

# COMMAND ----------

# import redshift_connector
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# DataFrame to Load into Redshift
df_order_details = spark.read.format("delta").table("databricks_lakehouse_demo_ws_e2.product_gold_db.orders_details")
df_order_details.count()



# COMMAND ----------

# df_order_details.filter("department_id = 5 and customer_state = 'CA' and customer_city = 'San Francisco'").count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Working Example for Redshift

# COMMAND ----------

# import redshift_connector
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# DataFrame to Load into Redshift
df_order_details = spark.read.format("delta") \
.table("databricks_lakehouse_demo_ws_e2.product_gold_db.orders_details") \
.filter("department_id = 5 and customer_state = 'CA' and customer_city = 'San Francisco'")

# Write to Redshift
df_order_details.write \
    .format("jdbc") \
    .option("url", "jdbc:redshift://databricks-demo-wg.*********.us-east-1.redshift-serverless.amazonaws.com:5439/dev") \
    .option("dbtable", "public.target_table") \
    .option("user", "*****") \
    .option("password", "***********") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .mode("append") \
    .save()