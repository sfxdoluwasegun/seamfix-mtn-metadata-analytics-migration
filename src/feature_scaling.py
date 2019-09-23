# Databricks notebook source
import numpy as np
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.ml.feature import PCA, StringIndexer, VectorAssembler, OneHotEncoder, IndexToString, Binarizer
from pyspark.ml.linalg import SparseVector, Vectors
from pyspark.sql.types import *
from pyspark.ml import Pipeline

# COMMAND ----------

# The ACCESS_KEY & SECRET_KEY could change due to monthly security modifications. 
BUCKET_NAME = "seamfix-machine-learning"
ACCESS_KEY = "AKIAU7APHSU5V6VGK7XV"
SECRET_KEY = "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"
temp_path = "s3://seamfix-machine-learning/mtn_xmlmetadata"

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", SECRET_KEY)

# COMMAND ----------

liveliness_df = spark.read.json("s3n://seamfix-machine-learning/mtn_xmlmetadata/livenessEvents_refined/*.json")
portrait_df = spark.read.json("s3n://seamfix-machine-learning/mtn_xmlmetadata/portrait_refined/*.json")
fingerprints_df = spark.read.json("s3n://seamfix-machine-learning/mtn_xmlmetadata/fingerprints_refined/*.json")
usecase_df = spark.read.json("s3n://seamfix-machine-learning/mtn_xmlmetadata/useCase_refined/*.json")
# root_df = spark.read.json("s3n://seamfix-machine-learning/mtn_xmlmetadata/root/*.json")

# COMMAND ----------

def encoder(df: DataFrame, col_name: str) -> (DataFrame, DataFrame):
    stringIndexer = StringIndexer(inputCol=col_name, outputCol="{}_{}".format(col_name, "dob")) # dob - double
    decimalModel = stringIndexer.fit(df)
    str_double = decimalModel.transform(df)
    str_double = str_double.drop(col_name)
    encoder = OneHotEncoder(inputCol="{}_{}".format(col_name, "dob"), outputCol="{}_{}".format(col_name, "ohe")) # ohe - oneHotEncode
    ohe = encoder.transform(str_double)
    ohe = ohe.drop(col_name, "{}_{}".format(col_name, "dob"))
    onehotEncoderPath = temp_path + "/onehotEncoder" + "/{}".format(get_df_name(df) + "/{}".format(col_name))
    encoder.write().overwrite().save(onehotEncoderPath)
    stringIndexerPath = temp_path + "/stringIndexer" + "/{}".format(get_df_name(df) + "/{}".format(col_name))
    stringIndexer.write().overwrite().save(stringIndexerPath)
    return ohe, str_double

# COMMAND ----------

def get_df_name(df: DataFrame) -> str:
    return [x for x in globals() if globals()[x] is df][0]
  
def get_df_unique_items(df: DataFrame):
  print("\n{} DataFrame \n".format(get_df_name(df)))
  for column in df.columns:
    print("{} - {}".format(column, df.toPandas()[column].unique()))

def print_uniqueColValues():
  df_list = [liveliness_df, portrait_df, fingerprints_df, usecase_df]
  for df in df_list:
    # Check unique_values.txt for detailed outline of unique values in each column
    get_df_unique_items(df)
  
def get_column_unique_items(df: DataFrame, col: str) -> np.ndarray:
  return df.toPandas()[col].unique()

def get_column_idx(df: DataFrame, column_name: str) -> (str, DataFrame):
  for (col, i) in zip(df.columns, range(len(df.columns))):
    if col == column_name:
      column_idx = "{}.columns[{}]".format(get_df_name(df), i)
  return eval(column_idx), eval(column_idx.split(".")[0])


  

