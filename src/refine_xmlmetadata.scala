// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.DataFrame

// COMMAND ----------

val AccessKey = "AKIAU7APHSU5V6VGK7XV"
val SecretKey = "YA3M5SbgZvZF2p9EQGveFEFxbr7Ox9KF48G/5XAB"
val EncodedSecretKey = SecretKey.replace("/", "%2F")
val AwsBucketName = "seamfix-machine-learning"
val MountName = "S3"
dbutils.fs.mount(s"s3a://$AccessKey:$EncodedSecretKey@$AwsBucketName", s"/mnt/$MountName")
// display(dbutils.fs.ls(s"/mnt/$MountName"))

val df_fp = spark.read.json("dbfs:/mnt/S3/mtn_xmlmetadata/fingerprints/*.json")
val df_li = spark.read.json("dbfs:/mnt/S3/mtn_xmlmetadata/livenessEvents/*.json")
val df_pt = spark.read.json("dbfs:/mnt/S3/mtn_xmlmetadata/portrait/*.json")
val df_uc = spark.read.json("dbfs:/mnt/S3/mtn_xmlmetadata/useCase/*.json")

// COMMAND ----------

def split_column(col_name: String, dataframe: DataFrame) : DataFrame = {
    val count = dataframe.select(size(col(col_name)).as("count")).agg(max("count")).collect()(0)(0).asInstanceOf[Int]
    val df = dataframe.select(col("*") +: (0 until count).map(i => col(col_name).getItem((i)).as(s"$col_name$i")): _* )//.select("fpDuration0.$numberLong")
    df
    }

def check_array_cols(dataframe: DataFrame) = {
  var cols = ArrayBuffer[String]()
  for( i <- 0 until dataframe.columns.length) {   
    val x = dataframe.schema(dataframe.columns(+i)).dataType match {
      case x: ArrayType => { 
        dataframe.withColumn(dataframe.columns(+i), col(dataframe.columns(+i)))
        cols += dataframe.columns(+i)
        }
      case _ => 
      }  
    }
  cols
  }  

// COMMAND ----------

val cols_fp = check_array_cols(df_fp)
val count_fp = cols_fp.length //count_fp: Int = 7

val cols_li = check_array_cols(df_li)
val count_li = cols_li.length //count_li: Int = 8

// Portrait does not contain an array column
// val cols_pt = check_array_cols(df_pt)
// val count_pt = cols_pt.length

// UseCase does not contain an array column
// val cols_uc = check_array_cols(df_uc)
// val count_uc = cols_uc.length

// COMMAND ----------

// Fingerprint de-arraylization with count_fp: Int = 7
val refined_fp = df_fp.select("hashID", "kitTag", "location", "fpDeviceType", "fpDuration.$numberLong", "fpNoOfAttempts", "fpProperty", "fpStatus", "fpSuccess", "fpValue", "uniqueId")
val init_fp = refined_fp.select($"hashID", $"kitTag", $"location", $"fpDeviceType", col("$numberLong").alias("fpDuration"), $"fpNoOfAttempts", $"fpProperty", $"fpStatus", $"fpSuccess", $"fpValue", $"uniqueId")
val refined_fp0 = split_column(cols_fp(0), init_fp)
val refined_fp0a = refined_fp0.drop(refined_fp0.col(cols_fp(0)))
val refined_fp1 = split_column(cols_fp(1), refined_fp0a)
val refined_fp1a = refined_fp1.drop(refined_fp1.col(cols_fp(1)))
val refined_fp2 = split_column(cols_fp(2), refined_fp1a)
val refined_fp2a = refined_fp2.drop(refined_fp2.col(cols_fp(2)))
val refined_fp3 = split_column(cols_fp(3), refined_fp2a) 
val refined_fp3a = refined_fp3.drop(refined_fp3.col(cols_fp(3)))
val refined_fp4 = split_column(cols_fp(4), refined_fp3a) 
val refined_fp4a = refined_fp4.drop(refined_fp4.col(cols_fp(4)))
val refined_fp5 = split_column(cols_fp(5), refined_fp4a) 
val refined_fp5a = refined_fp5.drop(refined_fp5.col(cols_fp(5)))
val refined_fp6 = split_column(cols_fp(6), refined_fp5a) 
val refined_fp6a = refined_fp6.drop(refined_fp6.col(cols_fp(6)))

// COMMAND ----------

// LivenessEvents de-arraylization with count_li: Int = 8
val refined_li = df_li.select("hashID", "kitTag", "location", "liCountAverage", "liDeviceType", "liDuration.$numberLong", "liProperty", "liStatus", "liSuccess", "liTimestamp", "liValue", "uniqueId")
val init_li = refined_li.select($"hashID", $"kitTag", $"location", $"liCountAverage", $"liDeviceType", col("$numberLong").alias("liDuration"), $"liProperty", $"liStatus", $"liSuccess", $"liTimestamp", $"liValue", $"uniqueId")
val refined_li0 = split_column(cols_li(0), init_li)
val refined_li0a = refined_li0.drop(refined_li0.col(cols_li(0)))
val refined_li1 = split_column(cols_li(1), refined_li0a)
val refined_li1a = refined_li1.drop(refined_li1.col(cols_li(1)))
val refined_li2 = split_column(cols_li(2), refined_li1a)
val refined_li2a = refined_li2.drop(refined_li2.col(cols_li(2)))
val refined_li3 = split_column(cols_li(3), refined_li2a) 
val refined_li3a = refined_li3.drop(refined_li3.col(cols_li(3)))
val refined_li4 = split_column(cols_li(4), refined_li3a) 
val refined_li4a = refined_li4.drop(refined_li4.col(cols_li(4)))
val refined_li5 = split_column(cols_li(5), refined_li4a) 
val refined_li5a = refined_li5.drop(refined_li5.col(cols_li(5)))
val refined_li6 = split_column(cols_li(6), refined_li5a) 
val refined_li6a = refined_li6.drop(refined_li6.col(cols_li(6)))
val refined_li7 = split_column(cols_li(7), refined_li6a) 
val refined_li7a = refined_li7.drop(refined_li7.col(cols_li(7)))

// COMMAND ----------

// UseCase de-arraylization
val refined_uc0 = df_uc.select("hashID", "kitTag", "location", "ucDuration.$numberLong", "ucProperty", "ucStatus", "ucSuccess", "ucValue", "uniqueId" )
val refined_uc = refined_uc0.select($"hashID", $"kitTag", $"location", col("$numberLong").alias("ucDuration"), $"ucProperty", $"ucStatus", $"ucSuccess", $"ucValue", $"uniqueId")

// Portrait de-arraylization
val refined_pt0 = df_pt.select("hashID", "kitTag", "location", "ptDeviceType", "ptDuration.$numberLong", "ptProperty", "ptStatus", "ptSuccess", "ptTimestamp", "ptValue", "uniqueId" )
val refined_pt = refined_pt0.select($"hashID", $"kitTag", $"location", $"ptDeviceType", col("$numberLong").alias("ptDuration"), $"ptProperty", $"ptStatus", $"ptSuccess", $"ptTimestamp", $"ptValue", $"uniqueId" )

// COMMAND ----------

// Saving DataFrames to AWS S3 in flattened JSON format
refined_fp6a.write.json("dbfs:/mnt/S3/mtn_xmlmetadata/fingerprints_refined")
refined_li7a.write.json("dbfs:/mnt/S3/mtn_xmlmetadata/livenessEvents_refined")
refined_pt.write.json("dbfs:/mnt/S3/mtn_xmlmetadata/portrait_refined")
refined_uc.write.json("dbfs:/mnt/S3/mtn_xmlmetadata/useCase_refined")

// COMMAND ----------

// STATS - Description of DataFrames
println("No. of columns in fingerprint dataframe is " + refined_fp6a.columns.length + " columns")
println("No. of columns in portrait dataframe is " + refined_pt.columns.length + " columns")
println("No. of columns in livenessEvents dataframe is " + refined_li7a.columns.length + " columns")
println("No. of columns in useCase dataframe is " + refined_uc.columns.length + " columns")
// Results:-
// No. of columns in fingerprint dataframe is 275 columns
// No. of columns in portrait dataframe is 11 columns
// No. of columns in livenessEvents dataframe is 1749 columns
// No. of columns in useCase dataframe is 9 columns

println("No. of rows in fingerprint dataframe is " + refined_fp6a.count() + " rows")
println("No. of rows in portrait dataframe is " + refined_pt.count() + " rows")
println("No. of rows in livenessEvents dataframe is " + refined_li7a.count() + " rows")
println("No. of rows in useCase dataframe is " + refined_uc.count() + " rows")
// Results:-
// No. of rows in fingerprint dataframe is 271697 rows
// No. of rows in portrait dataframe is 6795079 rows
// No. of rows in livenessEvents dataframe is 402 rows
// No. of rows in useCase dataframe is 15371 rows

// COMMAND ----------


