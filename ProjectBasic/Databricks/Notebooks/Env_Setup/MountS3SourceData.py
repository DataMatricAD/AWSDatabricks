# Databricks notebook source
access_key = "***********"
secret_key = "**************"
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "productorder-east2"
mount_name = "s3productordereast2"
dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")    
# file_location = f"/mnt/{mount_name}/categories/categories"
# df = spark.read.format("json").option("header", "true").load(file_location)
# df.printSchema()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/s3productordereast2

# COMMAND ----------

# df = spark.read.format("json").option("header", "true").load(f"s3a://{aws_bucket_name}/categories/categories")