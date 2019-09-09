# Databricks notebook source
import numpy
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.ml.feature import PCA, StringIndexer, VectorAssembler, OneHotEncoder, IndexToString
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

# df = spark.read.json("dbfs:/mnt/S3/mtn_xmlmetadata/METRIC_DATA_REPORT_NEW_view.json")
# df.createOrReplaceTempView("datasql")

# captureDate = spark.sql("""select METRIC_DATA.captureDate.`$date` as captureDate from datasql""") 
# get_unique_items(captureDate)

# COMMAND ----------

def get_df_name(df: DataFrame) -> str:
    return [x for x in globals() if globals()[x] is df][0]
  
def get_df_unique_items(df: DataFrame):
  print("\n{} DataFrame \n".format(get_df_name(df)))
  for column in df.columns:
    print("{} - {}".format(column, df.toPandas()[column].unique()))
    
def get_column_unique_items(df: DataFrame, col: str) -> numpy.ndarray:
  return df.toPandas()[col].unique()

def get_column_idx(df: DataFrame, column_name: str) -> (str, DataFrame):
  for (col, i) in zip(df.columns, range(len(df.columns))):
    if col == column_name:
      column_idx = "{}.columns[{}]".format(get_df_name(df), i)
  return eval(column_idx), eval(column_idx.split(".")[0])

def encoder(col_name: str, df: DataFrame) -> (DataFrame, DataFrame):
  stringIndexer = StringIndexer(inputCol=col_name, outputCol="{}_{}".format(col_name, "doub"))
  decimalModel = stringIndexer.fit(df)
  str_double = decimalModel.transform(df)
  str_double = str_double.drop(col_name)
  encoder = OneHotEncoder(inputCol="{}_{}".format(col_name, "doub"), outputCol="{}_{}".format(col_name, "ohe"))
  ohe = encoder.transform(str_double)
  ohe = ohe.drop(col_name)
  
  onehotEncoderPath = temp_path + "/onehotEncoder"
#   encoder.save(onehotEncoderPath)
  encoder.write().overwrite().save(onehotEncoderPath)
  
  stringIndexerPath = temp_path + "/stringIndexer"
#   stringIndexer.save(stringIndexerPath)
  stringIndexer.write().overwrite().save(stringIndexerPath)
  return str_double, ohe


# COMMAND ----------

df_list = [liveliness_df, portrait_df, fingerprints_df, usecase_df]
for item in df_list:
  # Check unique_values.txt for detailed outline of unique values in each column
  get_df_unique_items(item)
  

# COMMAND ----------

get_column_unique_items(fingerprints_df, "fpDeviceType14")

# COMMAND ----------

col_idx, dataframe = get_column_idx(fingerprints_df, "fpDeviceType0")
df1, _ = encoder(col_idx, dataframe)

# COMMAND ----------

stringIndexerPath = temp_path + "/stringIndexer"
loadedIndexer = StringIndexer.load(stringIndexerPath)
decimalModel = loadedIndexer.fit(fingerprints_df)
str_double = decimalModel.transform(fingerprints_df)

# COMMAND ----------

str_double.printSchema()

# COMMAND ----------

# type(stringIndexer) #pyspark.ml.feature.StringIndexer
# type(decimalModel) #pyspark.ml.feature.StringIndexerModel
# type(str_double) #pyspark.sql.dataframe.DataFrame

# COMMAND ----------

# workClassIndexer = StringIndexer().setInputCol("workclass").setOutputCol("workclass_indexed")
# workClassOneHot =  OneHotEncoder().setInputCol("workclass_indexed").setOutputCol("workclass_onehot")
# salaryIndexer = StringIndexer().setInputCol("salary").setOutputCol("label")

# vectorAssembler = VectorAssembler().setInputCols(['workclass_onehot','age']).setOutputCol("features")
# # create pipeline
# pipeline = Pipeline().setStages([workClassIndexer,workClassOneHot, salaryIndexer,vectorAssembler])


# COMMAND ----------

# df = sqlContext.createDataFrame([
#    (Vectors.dense([1, 2, 0]),),
#    (Vectors.dense([2, 0, 1]),),
#    (Vectors.dense([0, 1, 0]),)], ("features", ))

# pca = PCA(k=2, inputCol="features", outputCol="pca")
# model = pca.fit(df)
# transformed = model.transform(df)
