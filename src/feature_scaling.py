# Databricks notebook source
import numpy
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

def print_uniqueColValues():
  df_list = [liveliness_df, portrait_df, fingerprints_df, usecase_df]
  for df in df_list:
    # Check unique_values.txt for detailed outline of unique values in each column
    get_df_unique_items(df)
  
def get_column_unique_items(df: DataFrame, col: str) -> numpy.ndarray:
  return df.toPandas()[col].unique()

def get_column_idx(df: DataFrame, column_name: str) -> (str, DataFrame):
  for (col, i) in zip(df.columns, range(len(df.columns))):
    if col == column_name:
      column_idx = "{}.columns[{}]".format(get_df_name(df), i)
  return eval(column_idx), eval(column_idx.split(".")[0])

def encoder(df: DataFrame) -> (DataFrame, DataFrame):
  for col_name in df.columns:
    try:
      stringIndexer = StringIndexer(inputCol=col_name, outputCol="{}_{}".format(col_name, "dob")) # dob - double
      decimalModel = stringIndexer.fit(df)
      str_double = decimalModel.transform(df)
      str_double = str_double.drop(col_name)
      encoder = OneHotEncoder(inputCol="{}_{}".format(col_name, "dob"), outputCol="{}_{}".format(col_name, "ohe")) # ohe - oneHotEncode
      ohe = encoder.transform(str_double)
      ohe = ohe.drop(col_name, "{}_{}".format(col_name, "dob"))
      onehotEncoderPath = temp_path + "/onehotEncoder" + "/{}".format(get_df_name(df) + "/{}".format(col_name))
    #   encoder.save(onehotEncoderPath)
      encoder.write().overwrite().save(onehotEncoderPath)
      stringIndexerPath = temp_path + "/stringIndexer" + "/{}".format(get_df_name(df) + "/{}".format(col_name))
    #   stringIndexer.save(stringIndexerPath)
      stringIndexer.write().overwrite().save(stringIndexerPath)
    except:
      pass
    df = df
  return ohe, str_double


# COMMAND ----------

# portrait_df = portrait_df.withColumn("kitTag", portrait_df.select("kitTag"))
portrait_df = portrait_df.join(portrait_df.select("hashID", "kitTag"),joinType="outer")
a = portrait_df.select("hashID", "kitTag")
portrait_df = portrait_df.join(a, customersDF.hashID == ordersDF.hashID)



# COMMAND ----------

portrait_df.select("kitTag")

# COMMAND ----------

e, _ = encoder(portrait_df)

# COMMAND ----------

e.printSchema()

# COMMAND ----------

for column in liveliness_df.columns:
  col_idx, dataframe = get_column_idx(df, column)
  liveliness_df1, _ = encoder(col_idx, dataframe)
  
# for column in portrait_df.columns:
#   print(column)
#   col_idx, dataframe = get_column_idx(df, column)
#   portrait_df1, _ = encoder(col_idx, dataframe)
  
# for column in fingerprints_df.columns:
#   print(column)
#   col_idx, dataframe = get_column_idx(df, column)
#   fingerprints_df1, _ = encoder(col_idx, dataframe)
  
# for column in usecase_df.columns:
#   print(column)
#   col_idx, dataframe = get_column_idx(df, column)
#   usecase_df1, _ = encoder(col_idx, dataframe)

# COMMAND ----------

df1.select("fpDeviceType0_ohe").show()
# fingerprints_df.select("fpDeviceType0").show()

# COMMAND ----------

stringIndexerPath = temp_path + "/stringIndexer"
loadedIndexer = StringIndexer.load(stringIndexerPath)
decimalModel = loadedIndexer.fit(fingerprints_df)
str_double = decimalModel.transform(fingerprints_df)

# COMMAND ----------

f="w"
"{}".format(f) = 3

# COMMAND ----------

# type(stringIndexer) #pyspark.ml.feature.StringIndexer
# type(decimalModel) #pyspark.ml.feature.StringIndexerModel
# type(str_double) #pyspark.sql.dataframe.DataFrame
portrait_df.show()


# COMMAND ----------

locationClassIndexer = StringIndexer().setInputCol("location").setOutputCol("location_indexed")
locationClassOneHot =  OneHotEncoder().setInputCol("location_indexed").setOutputCol("location_onehot")
vectorAssembler = VectorAssembler().setInputCols(['location_onehot']).setOutputCol("features")

propertyIndexer = StringIndexer().setInputCol("ptProperty").setOutputCol("ptProperty_indexed")

# create pipeline
pipeline = Pipeline().setStages([locationClassIndexer,locationClassOneHot, propertyIndexer,vectorAssembler])


# COMMAND ----------

transformedDf = pipeline.fit(portrait_df).transform(portrait_df).select("features","ptProperty_indexed")
transformedDf.printSchema()

# COMMAND ----------

transformedDf.show()

# COMMAND ----------

# df = sqlContext.createDataFrame([
#    (Vectors.dense([1, 2, 0]),),
#    (Vectors.dense([2, 0, 1]),),
#    (Vectors.dense([0, 1, 0]),)], ("features", ))

# pca = PCA(k=2, inputCol="features", outputCol="pca")
# model = pca.fit(df)
# transformed = model.transform(df)

# COMMAND ----------

# df = sc.parallelize([[5.0, 'Prem', 'M', '12-21-2006 11:00:05','abc', '1'],
#                       [6.0, 'Kate', 'F', '05-30-2007 10:05:00', 'asdf', '2'],
#                       [3.0, 'Cheng', 'M', '12-30-2017 01:00:01', 'qwerty', '3']]).\
#     toDF(["age","name","sex","datetime_in_strFormat","initial_col_name","col_in_strFormat"])

binarizer = Binarizer(threshold=1.0, inputCol="values", outputCol="features")
binarizer.transform(df).show()

# COMMAND ----------

df = spark.createDataFrame([[9.5], [0.9], [0.7], [8.0]], ["values"])


# COMMAND ----------

df.show()

# COMMAND ----------


