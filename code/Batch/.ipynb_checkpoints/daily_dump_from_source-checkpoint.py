from pyspark.sql import SparkSession
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("Process and Insert Data into Daily Dump") \
    .enableHiveSupport() \
    .getOrCreate()

# Calculate previous day date
previous_day = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
base_hdfs_dir = "/user/itversity/raw_layer"

# Construct input path for the previous day's data
input_dir_pattern = f"{base_hdfs_dir}/{previous_day}/*"

branches_df = None
agents_df = None
transactions_df = None

print(f"Reading data from directories matching pattern: {input_dir_pattern}")

try:
    # Get all directories inside the previous day's folder
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    input_dirs = [status.getPath().toString() for status in fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(f"{base_hdfs_dir}/{previous_day}")) if status.isDirectory()]

    print(f"Found {len(input_dirs)} directories inside {previous_day}.")

    for input_dir in input_dirs:
        print(f"Processing directory: {input_dir}")
        
        # Read all files in the directory
        input_files = [status.getPath().toString() for status in fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(input_dir))]
        
        print(f"Found {len(input_files)} files in directory.")
        
        for input_file in input_files:
            print(f"Reading file: {input_file}")
            df = spark.read.format("csv").option("header", "true").load(input_file)
            
            # Drop 'source' and 'logs' columns if they exist
            if 'source' in df.columns:
                df = df.drop('source')
            if 'logs' in df.columns:
                df = df.drop('logs')
            
            # Ensure all DataFrames have the same schema before unioning
            if "branches" in input_file.lower():
                if branches_df is None:
                    branches_df = df
                    print("Reading branches data.")
                else:
                    branches_df = branches_df.union(df)
                    print("Union with existing branches data.")
            elif "agents" in input_file.lower():
                if agents_df is None:
                    agents_df = df
                    print("Reading agents data.")
                else:
                    agents_df = agents_df.union(df)
                    print("Union with existing agents data.")
            elif "transactions" in input_file.lower():
                if transactions_df is None:
                    transactions_df = df
                    print("Reading transactions data.")
                else:
                    transactions_df = transactions_df.union(df)
                    print("Union with existing transactions data.")
                    
except Exception as e:
    print(f"Error reading data: {e}")


if branches_df:
    branches_df = branches_df.dropDuplicates()
if agents_df:
    agents_df = agents_df.dropDuplicates()
if transactions_df:
    transactions_df = transactions_df.dropDuplicates()


if transactions_df:
    transactions_df.createOrReplaceTempView("transactions_view")
    print("Temporary view 'transactions_view' created successfully.")
if agents_df:
    agents_df.createOrReplaceTempView("agents_view")
    print("Temporary view 'agents_view' created successfully.")


sql_query = """
    SELECT 
        a.name AS sales_agent_name, 
        t.product_name, 
        SUM(t.units) AS total_sold_units
    FROM 
        transactions_view t
    JOIN 
        agents_view a ON t.sales_agent_id = a.sales_person_id
    GROUP BY 
        a.name, 
        t.product_name, 
        t.sales_agent_id, 
        t.product_id
"""

# Execute the query and store the result in a DataFrame
result_df = spark.sql(sql_query)



distinct_df = result_df.distinct()

previous_day = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
output_hdfs_path = f"/user/itversity/daily_dump_from_source/{previous_day}"

distinct_df.coalesce(1).write.mode("overwrite").csv(output_hdfs_path, header=True)
import subprocess

local_output_path = "itversity-material/project/daily_dump_from_source"

# Copy the directory from HDFS to the local file system
copy_command = ['hadoop', 'fs', '-get', '-f', output_hdfs_path, local_output_path]
copy_result = subprocess.run(copy_command, stdout=subprocess.PIPE,  stderr=subprocess.PIPE, check=True)

if copy_result.returncode == 0:
    print(f"Directory '{output_hdfs_path}' copied successfully to '{local_output_path}'.")
else:
    print(f"Error copying directory: {copy_result.stderr}")

# Stop Spark session
spark.stop()