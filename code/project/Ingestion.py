import os
import subprocess
from datetime import datetime

BASE_DIR = "itversity-material/project/data"
HDFS_DIR = "hdfs:///user/itversity/raw_layer"
LOG_FILE = "itversity-material/project/logs/move_files_to_hdfs.log"
INDEX_FILE = "itversity-material/project/logs/last_processed_index.txt"


now = datetime.now()
current_hour = now.strftime("%H")
current_day = now.strftime("%Y%m%d")

group_dirs = ["group1", "group2", "group3", "group4", "group5", "group6"]

# Read the last processed group index from the file
if os.path.exists(INDEX_FILE) and os.path.getsize(INDEX_FILE) > 0:
    with open(INDEX_FILE, "r") as index_file:
        last_index = int(index_file.read().strip())
else:
    last_index = -1 

# Determine the next group to process
next_index = (last_index + 1) % len(group_dirs)
group_dir = group_dirs[next_index]

# Update the last processed group index in the file
with open(INDEX_FILE, "w") as index_file:
    index_file.write(str(next_index))

# Create the target HDFS directory if it doesn't exist
hdfs_target_dir = f"{HDFS_DIR}/{current_day}/{current_hour}_{group_dir}"
mkdir_command = ["/opt/hadoop/bin/hdfs", "dfs", "-mkdir", "-p", hdfs_target_dir]
print(f"Executing: {' '.join(mkdir_command)}")  # Print the command being executed
subprocess.run(mkdir_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

group_path = os.path.join(BASE_DIR, group_dir)
if os.path.exists(group_path):
    for file_name in os.listdir(group_path):
        src_file_path = os.path.join(group_path, file_name)
        dest_file_path = f"{hdfs_target_dir}/{file_name.split('.')[0]}_{current_hour}_{current_day}.csv"
        put_command = ["/opt/hadoop/bin/hdfs", "dfs", "-put", src_file_path, dest_file_path]
        print(f"Executing: {' '.join(put_command)}")  # Print the command being executed
        result = subprocess.run(put_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if result.returncode == 0:
            log_message = f"{now} Successfully moved {src_file_path} to {dest_file_path}\n"
        else:
            log_message = f"{now} Failed to move {src_file_path} to HDFS: {result.stderr}\n"
        with open(LOG_FILE, "a") as log_file:
            log_file.write(log_message)
else:
    log_message = f"{now} Directory {group_path} does not exist\n"
    with open(LOG_FILE, "a") as log_file:
        log_file.write(log_message)
