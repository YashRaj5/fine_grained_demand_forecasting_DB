# Databricks notebook source
base_location = "dbfs:/tmp/demand_forecast/"

# COMMAND ----------

# MAGIC %md Set up user-scoped database location to avoid conflicts

# COMMAND ----------

# DBTITLE 1,Importing Libraries
import os
import re
from pathlib import Path

# COMMAND ----------

# DBTITLE 1,Checking if data is present
print("Checking if data is downloaded")
try:
    len(dbutils.fs.ls(base_location))
    print("Data is already downloaded!")
except:
    print("Downloading data!")
    dirname = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
    filename = "_get_data"
    data_generate_notebook_path = os.path.join(dirname, filename)
    dbutils.notebook.run(data_generate_notebook_path, 100)

# COMMAND ----------

# DBTITLE 1,Creating user-specific paths and database names
useremail = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
print("Username: "+useremail)

# COMMAND ----------

username_sql_compatible = re.sub('\W', '_', useremail.split('@')[0])
print("Username for SQL: "+username_sql_compatible)

# COMMAND ----------

tmp_data_path = f"{base_location.replace('dbfs:', '')}data/{useremail}/"
print("Temporary data path: "+tmp_data_path)

# COMMAND ----------

database_name = f"fine_grain_forecast_{username_sql_compatible}"
print("Database name: "+database_name)

# COMMAND ----------

# Creating user-scoped environment
spark.sql(f"DROP DATABASE IF EXISTS {database_name} CASCADE")
spark.sql(f"CREATE DATABASE {database_name} LOCATION '{tmp_data_path}'")
spark.sql(f"USE {database_name}")
print("User-scoped environment setup completed! Ready to go!")

# COMMAND ----------

Path(tmp_data_path).mkdir(parents=True, exist_ok=True)
