# Databricks notebook source
# DBTITLE 1,Initializing config
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Installing required Libraries
# MAGIC %pip install prophet

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1: Examine the Data
# MAGIC For our training dataset, we will make use of 5-years of store-item unit sales data for 50 items across 10 different stores. This data set is publicly available as part of a past Kaggle competition and can be downloaded with the `./00_config` notebook with your own Kaggle credentials.
# MAGIC
# MAGIC Once downloaded, we can unzip the train.csv.zip file and upload the decompressed CSV to /FileStore/demand_forecast/train/ using the file import steps documented [here](https://docs.databricks.com/data/databricks-file-system.html#!#user-interface). With the dataset accessible within Databricks, we can now explore it in preparation for modeling:

# COMMAND ----------

# DBTITLE 1,Access the Dataset
from pyspark.sql.types import *

# structure of training dataset
train_schema = StructType([
    StructField('date', DateType()),
    StructField('store', IntegerType()),
    StructField('item', IntegerType()),
    StructField('sales', IntegerType()),
])

# COMMAND ----------

# read the training file into a dataframe
train = spark.read.csv(
    f'{base_location}train/train.csv',
    header=True,
    schema=train_schema
).filter('year(date) BETWEEN 2013 AND 2017')

# COMMAND ----------

# making the dataframe queryable as a temporary view
train.createOrReplaceTempView('train')

# COMMAND ----------

# show data
display(train)

# COMMAND ----------

# MAGIC %md When performing demand forecasting, we are often interested in general trends and seasonality. Let's start our exploration by examining the annual trend in unit sales:

# COMMAND ----------

# DBTITLE 1,View Yearly Trends
# MAGIC %sql
# MAGIC  
# MAGIC SELECT
# MAGIC   year(date) as year, 
# MAGIC   sum(sales) as sales
# MAGIC FROM train
# MAGIC GROUP BY year(date)
# MAGIC ORDER BY year;

# COMMAND ----------

# MAGIC %md
# MAGIC It's very clear from the data that there is a generally upward trend in total unit sales across the stores. If we had better knowledge of the markets served by these stores, we might wish to identify whether there is a maximum growth capacity we'd expect to approach over the life of our forecast. But without that knowledge and by just quickly eyeballing this dataset, it feels safe to assume that if our goal is to make a forecast a few days, months or even a year out, we might expect continued linear growth over that time span.
# MAGIC
# MAGIC Now let's examine seasonality. If we aggregate the data around the individual months in each year, a distinct yearly seasonal pattern is observed which seems to grow in scale with overall growth in sales:

# COMMAND ----------

# DBTITLE 1,Monthly Trend
# MAGIC %sql
# MAGIC  
# MAGIC SELECT
# MAGIC   year(date) as year,
# MAGIC   month(date) as month, 
# MAGIC   sum(sales) as sales
# MAGIC FROM train
# MAGIC GROUP BY month(date), year(date)
# MAGIC ORDER BY year, month;
# MAGIC
# MAGIC -- SELECT 
# MAGIC --   TRUNC(date, 'MM') as month,
# MAGIC --   SUM(sales) as sales
# MAGIC -- FROM train
# MAGIC -- GROUP BY TRUNC(date, 'MM')
# MAGIC -- ORDER BY month;

# COMMAND ----------

# MAGIC %md
# MAGIC Aggregating the data at a weekday level, a pronounced weekly seasonal pattern is observed with a peak on Sunday (weekday 0), a hard drop on Monday (weekday 1), and then a steady pickup over the week heading back to the Sunday high. This pattern seems to be pretty stable across the five years of observations:
# MAGIC
# MAGIC **UPDATE** As part of the Spark 3 move to the (Proleptic Gregorian calendar)[https://databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html], the 'u' option in CAST(DATE_FORMAT(date, 'u') was removed. We are now using 'E to provide us a similar output.

# COMMAND ----------

# DBTITLE 1,View Weekday Trends
# MAGIC %sql
# MAGIC SELECT
# MAGIC   YEAR(date) as year,
# MAGIC   (
# MAGIC     CASE
# MAGIC       WHEN DATE_FORMAT(date, 'E') = 'Sun' THEN 0
# MAGIC       WHEN date_format(date, 'E') = 'Mon' THEN 1
# MAGIC       WHEN date_format(date, 'E') = 'Tue' THEN 2
# MAGIC       WHEN date_format(date, 'E') = 'Wed' THEN 3
# MAGIC       WHEN date_format(date, 'E') = 'Thu' THEN 4
# MAGIC       WHEN date_format(date, 'E') = 'Fri' THEN 5
# MAGIC       WHEN date_format(date, 'E') = 'Sat' THEN 6
# MAGIC     END     
# MAGIC   ) % 7 AS weekday,
# MAGIC   avg(sales) as sales
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC   date,
# MAGIC   sum(sales) as sales
# MAGIC   from train
# MAGIC   GROUP BY date   
# MAGIC ) x
# MAGIC GROUP BY year, weekday
# MAGIC ORDER BY year, weekday;

# COMMAND ----------

# MAGIC %sql
# MAGIC select date_format(date,  )
# MAGIC from train;

# COMMAND ----------


