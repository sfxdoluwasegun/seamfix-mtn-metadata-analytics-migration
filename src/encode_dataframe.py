# Databricks notebook source
import numpy as np
from pyspark.ml.feature import CountVectorizer
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml import Pipeline
from functools import reduce


# COMMAND ----------

BUCKET_NAME = "seamfix-machine-learning"
ACCESS_KEY = "AKIAU7APHSU5V6VGK7XV"
SECRET_KEY = "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"
temp_path =  "s3://seamfix-machine-learning/mtn_xmlmetadata"

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", SECRET_KEY)

# COMMAND ----------

li_path = "s3://seamfix-machine-learning/mtn_xmlmetadata/livenessEvents_refined/*.json"
pt_path = "s3://seamfix-machine-learning/mtn_xmlmetadata/portrait_refined/*.json"
fp_path = "s3://seamfix-machine-learning/mtn_xmlmetadata/fingerprints_refined/*.json"
uc_path = "s3://seamfix-machine-learning/mtn_xmlmetadata/useCase_refined/*.json"
pt_df = spark.read.json(pt_path)
li_df = spark.read.json(li_path)
uc_df = spark.read.json(uc_path)
fp_df = spark.read.json(fp_path)

# COMMAND ----------

def sort_columns(df: DataFrame) -> list:
  num_list = ["Duration", "Value", "Attempts", "CountAverage"]
  bool_list = ["Success", "Status"]
  str_list = ["hashID", "location", "Property", "uniqueId", "DeviceType", "Timestamp", "kitTag"]
  col_list = df.columns
  num_array = [(i, j) for i in col_list for j in num_list if j in i]
  bool_array = [(i, j) for i in col_list for j in bool_list if j in i]
  str_array = [(i, j) for i in col_list for j in str_list if j in i]
  return str_array, num_array, bool_array

def encode(df: DataFrame, col_list: list) -> DataFrame:
  df = df.fillna({ i : 0.0 for i in df.columns })
  preprocess = []
  for val_col in col_list:
    length = df.agg(approxCountDistinct(val_col[0])).first()[0]
    df = df.withColumn(val_col[0], split(col(val_col[0])," "))
    columnVectorizer = CountVectorizer(inputCol=val_col[0], outputCol='%s_'%(val_col[0]), vocabSize=length, minDF=1.0)
    preprocess += [columnVectorizer]
  pipeline = Pipeline(stages=preprocess)
  model = pipeline.fit(df)
  df = model.transform(df)
  df = refactor(df, col_list)
  return df

def refactor(df: DataFrame, col_list: list) -> DataFrame: 
  old_cols = [i+"_" for i in [i[0] for i in col_list]]
  new_cols = [i[0] for i in col_list]
  df = df.drop(*new_cols)
  df = reduce(lambda df, i: df.withColumnRenamed(old_cols[i], new_cols[i]), range(len(old_cols)), df)
  return df

def str_double(df: DataFrame, col_list: list) -> DataFrame:
#   To cast string numericall values to Doubles 
  for column in col_list:
    df = df.withColumn(column[0], df[column[0]].cast(DoubleType()))
  return df 

# COMMAND ----------

ptStr_cols, ptNum_cols, ptBool_cols = sort_columns(pt_df)
ucStr_cols, ucNum_cols, ucBool_cols = sort_columns(uc_df)
fpStr_cols, fpNum_cols, fpBool_cols = sort_columns(fp_df)

# COMMAND ----------

fp_df = str_double(fp_df, fpNum_cols)
fp_df = encode(fp_df, fpStr_cols)

# COMMAND ----------

pt_df = str_double(pt_df, ptNum_cols)
pt_df = encode(pt_df, ptStr_cols)

# COMMAND ----------

uc_df = str_double(uc_df, ucNum_cols)
uc_df = encode(uc_df, ucStr_cols)

# COMMAND ----------

fp_df.write.json("s3://seamfix-machine-learning/mtn_xmlmetadata/fingerprint_encoded")
uc_df.write.json("s3://seamfix-machine-learning/mtn_xmlmetadata/useCase_encoded")
pt_df.write.json("s3://seamfix-machine-learning/mtn_xmlmetadata/portrait_encoded")

# COMMAND ----------

cols_a = li_df.columns[:len(li_df.columns)//2] 
cols_b = li_df.columns[len(li_df.columns)//2:] 
li_df_a = li_df.select(cols_a) 
li_df_b = li_df.select(['hashID'] + cols_b) 

cols_a1 = li_df_a.columns[:len(li_df_a.columns)//2] 
cols_a2 = li_df_a.columns[len(li_df_a.columns)//2:] 
cols_b1 = li_df_b.columns[:len(li_df_b.columns)//2] 
cols_b2 = li_df_b.columns[len(li_df_b.columns)//2:] 
li_df_a1 = li_df_a.select(cols_a1) 
li_df_a2 = li_df_a.select(['hashID'] + cols_a2) 
li_df_b1 = li_df_b.select(cols_b1) 
li_df_b2 = li_df_b.select(['hashID'] + cols_b2) 

liStr_cols_a1, liNum_cols_a1, liBool_cols_a1 = sort_columns(li_df_a1)
liStr_cols_a2, liNum_cols_a2, liBool_cols_a2 = sort_columns(li_df_a2)
liStr_cols_b1, liNum_cols_b1, liBool_cols_b1 = sort_columns(li_df_b1)
liStr_cols_b2, liNum_cols_b2, liBool_cols_b2 = sort_columns(li_df_b2)

li_df_a1 = str_double(li_df_a1, liNum_cols_a1)
li_df_a1 = encode(li_df_a1, liStr_cols_a1)
li_df_a2 = str_double(li_df_a2, liNum_cols_a2)
li_df_a2 = encode(li_df_a2, liStr_cols_a2)
li_df_b1 = str_double(li_df_b1, liNum_cols_b1)
li_df_b1 = encode(li_df_b1, liStr_cols_b1)
li_df_b2 = str_double(li_df_b2, liNum_cols_b2)
li_df_b2 = encode(li_df_b2, liStr_cols_b2)

li_df_a1.write.json("s3://seamfix-machine-learning/mtn_xmlmetadata/livenessEvents_encoded/part1")
li_df_a2.write.json("s3://seamfix-machine-learning/mtn_xmlmetadata/livenessEvents_encoded/part2")
li_df_b1.write.json("s3://seamfix-machine-learning/mtn_xmlmetadata/livenessEvents_encoded/part3")
li_df_b2.write.json("s3://seamfix-machine-learning/mtn_xmlmetadata/livenessEvents_encoded/part4")


# # To Load LiveEvents and consolidate it as one dataframe because it was split into parts to enable easy encoding and upload to S3
# li_path1 = "s3://seamfix-machine-learning/mtn_xmlmetadata/livenessEvents_encoded/part1/*.json"
# li_path2 = "s3://seamfix-machine-learning/mtn_xmlmetadata/livenessEvents_encoded/part2/*.json"
# li_path3 = "s3://seamfix-machine-learning/mtn_xmlmetadata/livenessEvents_encoded/part3/*.json"
# li_path4 = "s3://seamfix-machine-learning/mtn_xmlmetadata/livenessEvents_encoded/part4/*.json"

# li_df_a1 = spark.read.json(li_path1)
# li_df_a2 = spark.read.json(li_path2)
# li_df_b1 = spark.read.json(li_path3)
# li_df_b2 = spark.read.json(li_path4)

# li_df_a2 = li_df_a2.withColumnRenamed('hashID', 'hashID_')
# li_df_1 = li_df_a1.join(li_df_a2, li_df_a1.hashID == li_df_a2.hashID_)
# li_df_1 = li_df_1.drop('hashID_')
# li_df_b1 = li_df_b1.withColumnRenamed('hashID', 'hashID_')
# li_df_2 = li_df_1.join(li_df_b1, li_df_1.hashID == li_df_b1.hashID_)
# li_df_2 = li_df_2.drop('hashID_')
# li_df_b2 = li_df_b2.withColumnRenamed('hashID', 'hashID_')
# li_df = li_df_2.join(li_df_b2, li_df_2.hashID == li_df_b2.hashID_)
# li_df = li_df.drop('hashID_')


