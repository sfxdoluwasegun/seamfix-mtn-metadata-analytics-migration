# Databricks notebook source
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *


# COMMAND ----------
# The ACCESS_KEY & SECRET_KEY could change due to monthly security modifications
BUCKET_NAME = "seamfix-machine-learning"
ROOT_DIR = "s3://seamfix-machine-learning/mtn_xmlmetadata/METRIC_DATA_REPORT_NEW_view.json"
ACCESS_KEY = "AKIAU7APHSU5V6VGK7XV"
SECRET_KEY = "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"

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", SECRET_KEY)

# COMMAND ----------

liveliness_df = spark.read.json("s3n://seamfix-machine-learning/mtn_xmlmetadata/livenessEvents_refined/*.json")
portrait_df = spark.read.json("s3n://seamfix-machine-learning/mtn_xmlmetadata/portrait_refined/*.json")
fingerprints_df = spark.read.json("s3n://seamfix-machine-learning/mtn_xmlmetadata/fingerprints_refined/*.json")
usecase_df = spark.read.json("s3n://seamfix-machine-learning/mtn_xmlmetadata/useCase_refined/*.json")


# COMMAND ----------

root_df = spark.read.json("s3n://seamfix-machine-learning/mtn_xmlmetadata/root/*.json")

# COMMAND ----------


df = spark.read.json("dbfs:/mnt/S3/mtn_xmlmetadata/METRIC_DATA_REPORT_NEW_view.json")
df.createOrReplaceTempView("datasql")

captureDate = spark.sql("""select METRIC_DATA.captureDate.`$date` as captureDate from datasql""") 





# COMMAND ----------

get_unique_items(captureDate)

# COMMAND ----------

def get_name(df: DataFrame) -> str:
    col_name =[x for x in globals() if globals()[x] is df][0]
    return col_name

def get_unique_items(df: DataFrame):
  print("\n{} DataFrame \n".format(get_name(df)))
  for column in df.columns:
    print("{} - {}".format(column, df.toPandas()[column].unique()))


# COMMAND ----------

df_list = [liveliness_df, portrait_df, fingerprints_df, usecase_df]
for item in df_list:
  # Check unique_values.txt for detailed outline of unique values in each column
  get_unique_items(item)

# COMMAND ----------

# liveliness_df.select('hashID').show()




