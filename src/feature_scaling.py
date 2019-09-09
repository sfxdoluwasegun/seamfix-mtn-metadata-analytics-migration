# Databricks notebook source
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
    col_name =[x for x in globals() if globals()[x] is df][0]
    return col_name

def get_unique_items(df: DataFrame):
  print("\n{} DataFrame \n".format(get_df_name(df)))
  for column in df.columns:
    print("{} - {}".format(column, df.toPandas()[column].unique()))

# COMMAND ----------

df_list = [liveliness_df, portrait_df, fingerprints_df, usecase_df]
for item in df_list:
  # Check unique_values.txt for detailed outline of unique values in each column
  get_unique_items(item)

# COMMAND ----------

def encoder(col_name: str, df: DataFrame) -> DataFrame:
  stringIndexer = StringIndexer(inputCol=col_name, outputCol="{}_{}".format(col_name, "doub"))
  decimalModel = stringIndexer.fit(df)
  str_double = decimalModel.transform(df)
  encoder = OneHotEncoder(inputCol="{}_{}".format(col_name, "doub"), outputCol="{}_{}".format(col_name, "ohe"))
  ohe = encoder.transform(str_double)
  
  onehotEncoderPath = temp_path + "/onehot-encoder"
#   encoder.save(onehotEncoderPath)
  encoder.write().overwrite().save(onehotEncoderPath)
  
  stringIndexerPath = temp_path + "/ohe-model"
#   stringIndexer.save(stringIndexerPath)
  stringIndexer.write().overwrite().save(stringIndexerPath)
  return str_double, ohe



# COMMAND ----------

def get_column_idx(df: DataFrame, column_name: str) -> str:
  for (col, i) in zip(df.columns, range(len(df.columns))):
    if col == column_name:
      column_idx = "{}.columns[{}]".format(get_df_name(df), i)
  return column_idx 

# COMMAND ----------

get_column_idx(portrait_df, "location")


# COMMAND ----------

df1, df2 = encoder(portrait_df.columns[5], portrait_df)

# COMMAND ----------

stringIndexer = StringIndexer(inputCol=portrait_df.columns[5], outputCol="{}_{}".format(portrait_df.columns[5], "doub"))
decimalModel = stringIndexer.fit(fingerprints_df)
str_double = decimalModel.transform(fingerprints_df)

# COMMAND ----------

type(stringIndexer) #pyspark.ml.feature.StringIndexer
type(decimalModel) #pyspark.ml.feature.StringIndexerModel
type(str_double) #pyspark.sql.dataframe.DataFrame

# COMMAND ----------

# def reverse_encoder() 
stringIndexerPath = temp_path + "/ohe-model"
loadedIndexer = StringIndexer.load(stringIndexerPath)
decimalModel = loadedIndexer.fit(usecase_df)
str_double = decimalModel.transform(usecase_df)



# convertor = IndexToString(inputCol='df.columns[-2]', outputCol='predictedLabel', labels=stringIndexer.labels)

# COMMAND ----------

liveliness_df.select(liveliness_df.columns[-1]).show()

# COMMAND ----------

# type(loadedIndexer) #pyspark.ml.feature.StringIndexer
# type(decimalModel) #pyspark.ml.feature.StringIndexerModel
# type(str_double) #pyspark.sql.dataframe.DataFrame

# COMMAND ----------

df2.select("ptProperty_doub", "ptProperty_ohe").show()


# COMMAND ----------

# encoder.transform(td).head().features
# en = encoder.transform(td)#.head().features
# en.printSchema()
# en.select("ptProperty", "ptProperty_idx", "features").show(100)
# en.agg({"ptProperty_idx": "max"}).collect()[0]

encoder.setParams(outputCol="freqs").transform(td).head().freqs
# en_0 = encoder.setParams(outputCol="freqs").transform(td)#.head().freqs
# en_0.select("ptProperty", "ptProperty_idx", "freqs").show(100)
# en_0.head().freqs

params = {encoder.dropLast: False, encoder.outputCol: "test"}
# encoder.transform(td, params).head().test
en_1 = encoder.transform(td, params)#.head().test
en_1.select("ptProperty", "ptProperty_idx", "test").show(100)

onehotEncoderPath = temp_path + "/onehot-encoder"
encoder.save(onehotEncoderPath)



# COMMAND ----------

loadedEncoder = OneHotEncoder.load(onehotEncoderPath)

# loadedEncoder.getDropLast() == encoder.getDropLast()

en_2 = loadedEncoder.transform(td, params)

# COMMAND ----------

en_2.select("ptProperty", "ptProperty_idx", "test").show(100)



# model
# modelPath = temp_path + "/ohe-model"
# >>> model.save(modelPath)

# COMMAND ----------

# td.show(5)
# td.select("ptProperty", "ptProperty_idx").show(100)
td.printSchema()

# COMMAND ----------

def one_hot(series):
    label_binarizer = pp.LabelBinarizer()
    label_binarizer.fit(range(max(series)+1))
    return label_binarizer.transform(series)

# COMMAND ----------

workClassIndexer = StringIndexer().setInputCol("workclass").setOutputCol("workclass_indexed")
workClassOneHot =  OneHotEncoder().setInputCol("workclass_indexed").setOutputCol("workclass_onehot")
salaryIndexer = StringIndexer().setInputCol("salary").setOutputCol("label")

vectorAssembler = VectorAssembler().setInputCols(['workclass_onehot','age']).setOutputCol("features")
# create pipeline
pipeline = Pipeline().setStages([workClassIndexer,workClassOneHot, salaryIndexer,vectorAssembler])




# COMMAND ----------

df = sqlContext.createDataFrame([
   (Vectors.dense([1, 2, 0]),),
   (Vectors.dense([2, 0, 1]),),
   (Vectors.dense([0, 1, 0]),)], ("features", ))

# pca = PCA(k=2, inputCol="features", outputCol="pca")
# model = pca.fit(df)
# transformed = model.transform(df)

# COMMAND ----------

temp = portrait_df.rdd.map(lambda x:[float(y) for y in x['all_features']]).toDF(portrait_df.columns)



# COMMAND ----------



# COMMAND ----------


