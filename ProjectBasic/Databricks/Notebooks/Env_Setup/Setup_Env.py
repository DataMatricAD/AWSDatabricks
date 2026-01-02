# Databricks notebook source
# MAGIC %md
# MAGIC ###Method - Source To Bronze Table Load and Bronze Table Creation

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, col, to_date

def ingest_json_to_delta(source_path: str, database_name: str, table_name: str):
    """
    Generic framework to read JSON files from S3, infer schema, and write to a partitioned Delta table in Unity Catalog.
    
    Parameters:
        source_path (str): Databricks-mounted S3 path where JSON files are stored.
        database_name (str): The target Unity Catalog database (e.g., bronze).
        table_name (str): The target Delta table name.
    """

    spark = SparkSession.builder.appName("JSON_to_Delta_Ingestion").getOrCreate()

    # Step 1: Read JSON files from the source path (S3) and include the source file name
    df = spark.read.format("json").option("inferSchema", "true").load(source_path) \
        .withColumn("source_file", input_file_name())  # Extract file name

    # Step 2: Add metadata columns
    df = df.withColumn("ingestion_time", current_timestamp())  # Timestamp when data is loaded
    df = df.withColumn("date_partition", to_date(col("ingestion_time")))  # Partition column (daily)

    # Step 3: Define full table path (Unity Catalog format)
    full_table_name = f"{database_name}.{table_name}"

    # Step 4: Write to Delta Table (Create if not exists, Append if exists) with Partitioning
    df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("date_partition") \
        .saveAsTable(full_table_name)

    print(f"Data successfully ingested into {full_table_name} with daily partitioning.")

# COMMAND ----------

# dbfs:/mnt/s3productordereast2/categories/
# dbfs:/mnt/s3productordereast2/customers/
# dbfs:/mnt/s3productordereast2/departments/
# dbfs:/mnt/s3productordereast2/order_items/
# dbfs:/mnt/s3productordereast2/orders/
# dbfs:/mnt/s3productordereast2/products/

# COMMAND ----------

# # Example Usage
# source_path = "dbfs:/mnt/s3productordereast2/categories/" 
# database_name = "databricks_lakehouse_demo_ws_e2.product_bronze_db"
# table_name = "categories"

# ingest_json_to_delta(source_path, database_name, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Method - Bronze To Silver Table Load and Silver Table Creation

# COMMAND ----------

from delta.tables import DeltaTable

def load_bronze_to_silver(bronze_table, silver_table, primary_key, selected_columns_str):

    selected_columns =  selected_columns_str.split(",")
    # Step 1: Read the bronze table
    bronze_df = spark.read.table(bronze_table).select(selected_columns)
    
    # Step 2: Clean the data (if needed)
    cleaned_df = bronze_df.dropDuplicates([primary_key])

    # Step 4: Check if Silver table exists
    if spark._jsparkSession.catalog().tableExists(silver_table):
        # Step 3: Read the silver table
        # silver_df = spark.read.table(silver_table)
        
        # Step 4: Create DeltaTable object for the silver table
        delta_silver_table = DeltaTable.forName(spark, silver_table)
        
        # Step 5: Merge (Insert new records, Update existing)
        delta_silver_table.alias("silver") \
            .merge(
                cleaned_df.alias("bronze"),
                f"silver.{primary_key} = bronze.{primary_key}"
            ) \
            .whenMatchedUpdate(set={
                col_name: f"bronze.{col_name}" for col_name in selected_columns if col_name != primary_key
            }) \
            .whenNotMatchedInsert(values={
                col_name: f"bronze.{col_name}" for col_name in selected_columns
            }) \
            .execute()
    else:
        # Step 6: Create Silver table if not exists, adding insert_date column
        final_df = cleaned_df.withColumn("insert_date", current_timestamp())

        final_df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(silver_table)

        print(f"Created {silver_table} and inserted initial data.")

# COMMAND ----------


# bronze_table = "databricks_lakehouse_demo_ws_e2.product_bronze_db.categories"
# silver_table = "databricks_lakehouse_demo_ws_e2.product_silver_db.categories"
# primary_key = "category_id"

# selected_cols = ["category_department_id", "category_id", "category_name"]
# load_bronze_to_silver(bronze_table, silver_table, primary_key, selected_cols)


# COMMAND ----------

# # Get the path of the current notebook
# current_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# # Print the path
# print(f"Current Notebook Path: {current_notebook_path}")
