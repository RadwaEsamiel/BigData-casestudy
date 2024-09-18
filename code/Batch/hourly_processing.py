from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import os
spark = SparkSession.builder \
    .appName("Data Processing and Insertion") \
    .enableHiveSupport() \
    .getOrCreate()
# Define base HDFS directory
base_hdfs_dir = "/user/itversity/raw_layer"
# Calculate current date and previous hour
current_dt = datetime.now()

# Handle special case when current time is 12am (midnight)
if current_dt.hour == 0:
    # Adjust processing date to previous day
    processing_date = (current_dt - timedelta(days=1)).strftime("%Y%m%d")
    prev_hour_str = "23"  # Previous hour is 11pm
else:
    processing_date = current_dt.strftime("%Y%m%d")
    prev_hour_str = (current_dt - timedelta(hours=2)).strftime("%H")

    # Construct input path in HDFS
input_dir_pattern = f"{base_hdfs_dir}/{processing_date}/{prev_hour_str}_*"

# Get all directories matching the input pattern
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
input_dirs = [status.getPath().toString() for status in fs.globStatus(spark._jvm.org.apache.hadoop.fs.Path(input_dir_pattern))]
# Lists to store DataFrames for each category
branches_dfs = []
agents_dfs = []
transactions_dfs = []
# Load existing data from Hive tables
branches_hive_df = spark.table("BigData_DWH.branches_dimension")
agents_hive_df = spark.table("BigData_DWH.sales_agents_dimension")
transactions_hive_df = spark.table("BigData_DWH.Transactions_Fact_table")
for input_dir in input_dirs:
    # Read all files in the directory
    input_files = [status.getPath().toString() for status in fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(input_dir))]

    for input_file in input_files:
        # Extract the file name from the input path
        input_file_name = os.path.basename(input_file)

        # Read the DataFrame from CSV
        df = spark.read.format("csv").option("header", "true").load(input_file)

        # Show the DataFrame (for verification)
        print(f"DataFrame from {input_file_name}:")
        df.show()

        # Determine DataFrame name based on file name content
        if "branches" in input_file_name.lower():
            branches_dfs.append(df)
        elif "agents" in input_file_name.lower():
            agents_dfs.append(df)
        elif "transactions" in input_file_name.lower():
            transactions_dfs.append(df)
        else:
            continue  # Skip processing for unknown files

if branches_dfs:
    branches_new_df = branches_dfs[0]
    for df in branches_dfs[1:]:
        branches_new_df = branches_new_df.union(df)

    branches_new_df = branches_new_df.dropDuplicates()
    branches_new_df = branches_new_df.join(branches_hive_df, on="branch_id", how="left_anti")
    print("branches DataFrame:")
    branches_new_df.show()

if agents_dfs:
    agents_new_df = agents_dfs[0]
    for df in agents_dfs[1:]:
        agents_new_df = agents_new_df.union(df)

    agents_new_df = agents_new_df.dropDuplicates()
    agents_new_df = agents_new_df.join(agents_hive_df, on="sales_person_id", how="left_anti")
    
    print("agents DataFrame:")
    agents_new_df.show()
# Print column names of transactions_dfs
if transactions_dfs:
    transactions_new_df = transactions_dfs[0]
    for df in transactions_dfs[1:]:
        transactions_new_df = transactions_new_df.union(df)

    transactions_new_df = transactions_new_df.dropDuplicates()
    transactions_new_df = transactions_new_df.join(transactions_hive_df, on="transaction_id", how="left_anti")
    
    print("Transactions DataFrame:")
    transactions_new_df.show()
    
    # Get column names
    column_names = transactions_new_df.columns
    print("Column names:")
    for col in column_names:
        print(col)

# Columns to be moved to a new DataFrame
columns_to_move = ["customer_id", "customer_fname", "cusomter_lname", "cusomter_email"]

# Create a new DataFrame with the selected columns
customer_df = transactions_new_df.select(columns_to_move)
print("Customer DataFrame:")
customer_df.show()


customer_df.createOrReplaceTempView("customer_dim")
# SQL query to get the most selling products

customer_dim = """
SELECT * FROM customer_dim
"""

customer_dim = spark.sql(customer_dim)

customer_dim.show()
spark.sql("INSERT INTO BigData_DWH.customers_dimension SELECT * FROM customer_dim")

# Columns to be moved to a new DataFrame
columns_to_move = ["product_id", "product_name", "product_category"]

# Create a new DataFrame with the selected columns
product_df = transactions_new_df.select(columns_to_move)
print("Customer DataFrame:")
product_df.show()

product_df.createOrReplaceTempView("product_Dim")
spark.sql("INSERT INTO BigData_DWH.products_dimension SELECT * FROM product_Dim")

from pyspark.sql.functions import when, col, coalesce

transactions_new_df = transactions_new_df.withColumn(
    "offer",
    coalesce(
        when(col("offer_1") == "True", "offer_1"),
        when(col("offer_2") == "True", "offer_2"),
        when(col("offer_3") == "True", "offer_3"),
        when(col("offer_4") == "True", "offer_4"),
        when(col("offer_5") == "True", "offer_5")
    )
)

# Drop the original offer columns and other specified columns
transactions_new_df = transactions_new_df.drop("offer_1", "offer_2", "offer_3", "offer_4", "offer_5")

# Show the transformed DataFrame
transactions_new_df.show()
transactions_new_df = transactions_new_df.drop("logs", "source")
# Get column names
column_names = transactions_new_df.columns
print("Column names:")
for col in column_names:
    print(col)

from pyspark.sql.functions import when, col, coalesce,expr

# Calculate Total_price as DOUBLE
transactions_new_df = transactions_new_df.withColumn(
    "Total_price",
     col("units") * col("unit_price")
)

# Show the updated DataFrame
transactions_new_df.show()


    # Add 'discount_percentage' column
transactions_new_df = transactions_new_df.withColumn(
        "discount_percentage",
        when(col("offer") == "offer_1", 0.05)
        .when(col("offer") == "offer_2", 0.10)
        .when(col("offer") == "offer_3", 0.15)
        .when(col("offer") == "offer_4", 0.20)
        .when(col("offer") == "offer_5", 0.25)
        .otherwise(0.0)
    )

    # Add 'Total_price_paid_after_discount' column
transactions_new_df = transactions_new_df.withColumn(
        "Total_price_after_discount",
        col("Total_price") * (1 - col("discount_percentage"))
    )

    # Show the transformed DataFrame
transactions_new_df.show()

column_names = transactions_new_df.columns
print("Column names:")
for col in column_names:
    print(col)


# Columns to be moved to a new DataFrame
columns_to_move = ["transaction_id", "unit_price", "is_online", "payment_method", "shipping_address", "offer"]

# Create a new DataFrame with the selected columns
orders_df = transactions_new_df.select(columns_to_move)
print("orders DataFrame:")
orders_df.show()

orders_df.createOrReplaceTempView("orders_Dim")
spark.sql("INSERT INTO BigData_DWH.orders_dimension SELECT * FROM orders_Dim")


from pyspark.sql.functions import when, col, coalesce, expr

# Select only the necessary columns
transactions_new_dsf = transactions_new_df.select(
    "transaction_date",
    "transaction_id",
    "customer_id",
    "sales_agent_id",
    "branch_id",
    "product_id",
    "units",
    "Total_price",
    "discount_percentage",
    "Total_price_after_discount"
)
transactions_new_dsf.show()

# Perform casting
transactions_new_dsf = transactions_new_dsf.withColumn(
    "units",
    col("units").cast("int")
)

transactions_new_dsf.createOrReplaceTempView("transactions_Fact")
branches_new_df.createOrReplaceTempView("branches_view")
agents_new_df.createOrReplaceTempView("agents_view")


spark.sql("""
    INSERT INTO BigData_DWH.Transactions_Fact_table
    PARTITION (transaction_date)
    SELECT
        transaction_id,
        customer_id,
        sales_agent_id,
        branch_id,
        product_id,
        units,
        Total_price,
        discount_percentage,
        Total_price_after_discount,
        transaction_date
    FROM transactions_Fact
""")


spark.sql("INSERT INTO BigData_DWH.branches_dimension SELECT * FROM branches_view")

spark.sql("INSERT INTO BigData_DWH.sales_agents_dimension SELECT * FROM agents_view")
# Stop Spark session
spark.stop()