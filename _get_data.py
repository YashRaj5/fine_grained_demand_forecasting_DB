# Databricks notebook source
# MAGIC %md
# MAGIC Now we are going to download data from kaggle.

# COMMAND ----------

# MAGIC %pip install kaggle

# COMMAND ----------

# MAGIC %md
# MAGIC Setting kaggle credentails

# COMMAND ----------

import os

# COMMAND ----------

os.environ['kaggle_username'] = dbutils.secrets.get("kaggle-cred", "kaggle-username")
os.environ['kaggle_key'] = dbutils.secrets.get("kaggle-cred", "kaggle-key")

# COMMAND ----------

# MAGIC %md Downloading data from kaggle using the credentials set above.

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd /databricks/driver
# MAGIC export KAGGLE_USERNAME=$kaggle_username
# MAGIC export KAGGLE_KEY=$kaggle_key
# MAGIC kaggle competitions download -c demand-forecasting-kernels-only
# MAGIC unzip -o demand-forecasting-kernels-only.zip

# COMMAND ----------

# MAGIC %md Moving data to common location

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/", "dbfs:/tmp/demand_forecast/train/train.csv", True)
