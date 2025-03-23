# Databricks notebook source
#  %sql
#  WITH OrderSummary AS (
#         SELECT 
#             o.order_id,
#             o.order_date,
#             o.order_status,
#             o.order_customer_id,
#             oi.order_item_product_id,
#             oi.order_item_quantity,
#             oi.order_item_subtotal,
#             p.product_name,
#             p.product_price,
#             p.product_category_id,
#             c.category_department_id,
#             c.category_id,
#             c.category_name,
#             d.department_id,
#             d.department_name,
#             cu.customer_id,
#             cu.customer_fname,
#             cu.customer_lname,
#             cu.customer_email,
#             cu.customer_city,
#             cu.customer_state
#         FROM databricks_lakehouse_demo_ws_e2.product_silver_db.orders o
#         JOIN databricks_lakehouse_demo_ws_e2.product_silver_db.order_items oi ON o.order_id = oi.order_item_order_id
#         JOIN databricks_lakehouse_demo_ws_e2.product_silver_db.products p ON oi.order_item_product_id = p.product_id
#         JOIN databricks_lakehouse_demo_ws_e2.product_silver_db.categories c ON p.product_category_id = c.category_id
#         JOIN databricks_lakehouse_demo_ws_e2.product_silver_db.departments d ON c.category_department_id = d.department_id
#         JOIN databricks_lakehouse_demo_ws_e2.product_silver_db.customers cu ON o.order_customer_id = cu.customer_id
#     )
#     -- Aggregate Data
#     SELECT
#         os.department_id,
#         os.department_name,
#         os.category_id,
#         os.category_name,
#         os.customer_id,
#         os.customer_fname,
#         os.customer_lname,
#         os.customer_email,
#         os.customer_city,
#         os.customer_state,
#         COUNT(DISTINCT os.order_id) AS total_orders,
#         SUM(os.order_item_quantity) AS total_quantity_sold,
#         SUM(os.order_item_subtotal) AS total_revenue,
#         AVG(os.order_item_subtotal) AS avg_order_value,
#         CURRENT_TIMESTAMP() AS update_timestamp
#     FROM OrderSummary os
#     GROUP BY
#         os.department_id, os.department_name,
#         os.category_id, os.category_name,
#         os.customer_id, os.customer_fname, os.customer_lname,
#         os.customer_email, os.customer_city, os.customer_state

# COMMAND ----------

merge_query = """
MERGE INTO databricks_lakehouse_demo_ws_e2.product_gold_db.orders_details AS target
USING (
    -- Aggregated data source
    WITH OrderSummary AS (
        SELECT 
            o.order_id,
            o.order_date,
            o.order_status,
            o.order_customer_id,
            oi.order_item_product_id,
            oi.order_item_quantity,
            oi.order_item_subtotal,
            p.product_name,
            p.product_price,
            p.product_category_id,
            c.category_department_id,
            c.category_id,
            c.category_name,
            d.department_id,
            d.department_name,
            cu.customer_id,
            cu.customer_fname,
            cu.customer_lname,
            cu.customer_email,
            cu.customer_city,
            cu.customer_state
        FROM databricks_lakehouse_demo_ws_e2.product_silver_db.orders o
        JOIN databricks_lakehouse_demo_ws_e2.product_silver_db.order_items oi ON o.order_id = oi.order_item_order_id
        JOIN databricks_lakehouse_demo_ws_e2.product_silver_db.products p ON oi.order_item_product_id = p.product_id
        JOIN databricks_lakehouse_demo_ws_e2.product_silver_db.categories c ON p.product_category_id = c.category_id
        JOIN databricks_lakehouse_demo_ws_e2.product_silver_db.departments d ON c.category_department_id = d.department_id
        JOIN databricks_lakehouse_demo_ws_e2.product_silver_db.customers cu ON o.order_customer_id = cu.customer_id
    )
    -- Aggregate Data
    SELECT
        os.department_id,
        os.department_name,
        os.category_id,
        os.category_name,
        os.customer_id,
        os.customer_fname,
        os.customer_lname,
        os.customer_email,
        os.customer_city,
        os.customer_state,
        COUNT(DISTINCT os.order_id) AS total_orders,
        SUM(os.order_item_quantity) AS total_quantity_sold,
        SUM(os.order_item_subtotal) AS total_revenue,
        AVG(os.order_item_subtotal) AS avg_order_value,
        CURRENT_TIMESTAMP() AS update_timestamp
    FROM OrderSummary os
    GROUP BY
        os.department_id, os.department_name,
        os.category_id, os.category_name,
        os.customer_id, os.customer_fname, os.customer_lname,
        os.customer_email, os.customer_city, os.customer_state
) AS source
ON target.customer_id = source.customer_id 
AND target.category_id = source.category_id 
AND target.department_id = source.department_id

-- Update existing records
WHEN MATCHED THEN
UPDATE SET 
    target.department_name = source.department_name,
    target.category_name = source.category_name,
    target.customer_fname = source.customer_fname,
    target.customer_lname = source.customer_lname,
    target.customer_email = source.customer_email,
    target.customer_city = source.customer_city,
    target.customer_state = source.customer_state,
    target.total_orders = source.total_orders,
    target.total_quantity_sold = source.total_quantity_sold,
    target.total_revenue = source.total_revenue,
    target.avg_order_value = source.avg_order_value,
    target.update_timestamp = source.update_timestamp

-- Insert new records
WHEN NOT MATCHED THEN 
INSERT (
    department_id, department_name,
    category_id, category_name,
    customer_id, customer_fname, customer_lname, customer_email, 
    customer_city, customer_state, total_orders, total_quantity_sold, 
    total_revenue, avg_order_value, update_timestamp
) 
VALUES (
    source.department_id, source.department_name,
    source.category_id, source.category_name,
    source.customer_id, source.customer_fname, source.customer_lname, source.customer_email,
    source.customer_city, source.customer_state, source.total_orders, source.total_quantity_sold,
    source.total_revenue, source.avg_order_value, source.update_timestamp
);

"""

# COMMAND ----------

# Check and create table if not exists
if not spark._jsparkSession.catalog().tableExists("databricks_lakehouse_demo_ws_e2.product_gold_db.orders_details"):
    spark.sql("""
    CREATE TABLE databricks_lakehouse_demo_ws_e2.product_gold_db.orders_details (
        department_id INT,
        department_name STRING,
        category_id INT,
        category_name STRING,
        customer_id INT,
        customer_fname STRING,
        customer_lname STRING,
        customer_email STRING,
        customer_city STRING,
        customer_state STRING,
        total_orders INT,
        total_quantity_sold INT,
        total_revenue DECIMAL(18,2),
        avg_order_value DECIMAL(18,2),
        update_timestamp TIMESTAMP
    ) USING DELTA;
    """)
    print("Table orders_details created.")

# Perform Upsert (Merge)
spark.sql(merge_query)

# Optimize for better performance
spark.sql("OPTIMIZE databricks_lakehouse_demo_ws_e2.product_gold_db.orders_details ZORDER BY (customer_id, category_id, department_id)")

print("Delta Table Updated & Optimized Successfully!")
