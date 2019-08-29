// Databricks notebook source
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.functions._

// COMMAND ----------

val AccessKey = "AKIAU7APHSU5V6VGK7XV"
val SecretKey = "YA3M5SbgZvZF2p9EQGveFEFxbr7Ox9KF48G/5XAB"
val EncodedSecretKey = SecretKey.replace("/", "%2F")
val AwsBucketName = "seamfix-machine-learning"
val MountName = "S3"
dbutils.fs.mount(s"s3a://$AccessKey:$EncodedSecretKey@$AwsBucketName", s"/mnt/$MountName")
// display(dbutils.fs.ls(s"/mnt/$MountName"))

// COMMAND ----------


val df = spark.read.json("dbfs:/mnt/S3/mtn_xmlmetadata/METRIC_DATA_REPORT_NEW_view.json")
df.createOrReplaceTempView("datasql")

val root = spark.sql("""select 
                from_unixtime(substring(METRIC_DATA.captureDate.*, 1, 10)) as captureDate, 
                METRIC_DATA.kitTag,
                METRIC_DATA.location,
                METRIC_DATA.uniqueId,
                _id.`$oid` as hashID
               	from datasql""")

val fingerprints = spark.sql("""select
                       	from_unixtime(substring(METRIC_DATA.captureDate.*, 1, 10)) as captureDate, 
                       	METRIC_DATA.kitTag,
                       	METRIC_DATA.location,
                       	METRIC_DATA.uniqueId,
                       	_id.`$oid` as hashID,
                       	METRIC_DATA.metricBreakdowns.fingerprints.fingerprint.attempts, 
                       	METRIC_DATA.metricBreakdowns.fingerprints.fingerprint.noOfAttempts 
                      	from datasql""")
.withColumn("exploded", explode($"attempts"))
.select($"uniqueId", $"hashID", $"captureDate", $"location", $"kitTag", $"exploded.attemptBreakdowns", $"exploded.deviceType", $"exploded.duration", $"exploded.status", $"noOfAttempts")
.withColumn("explodedBreakdown", explode($"attemptBreakdowns"))
.select($"uniqueId", $"hashID", $"captureDate", $"location", $"kitTag", $"explodedBreakdown.property".alias("fpProperty"), $"explodedBreakdown.success".alias("fpSuccess"), $"explodedBreakdown.value".alias("fpValue"), $"deviceType".alias("fpDeviceType"), $"duration".alias("fpDuration"), $"status".alias("fpStatus"), $"noOfAttempts".alias("fpNoOfAttempts"))

val livenessEvents = spark.sql("""select
	                       from_unixtime(substring(METRIC_DATA.captureDate.*, 1, 10)) as captureDate, 
	                       METRIC_DATA.kitTag,
	                       METRIC_DATA.location,
	                       METRIC_DATA.uniqueId,
	                       _id.`$oid` as hashID,
	                       METRIC_DATA.metricBreakdowns.livenessEvents.livenessEvent.attempts 
	                       from datasql""")
.withColumn("exploded", explode($"attempts"))
.select($"uniqueId", $"hashID", $"captureDate", $"location", $"kitTag", $"exploded.attemptBreakdowns", $"exploded.countAverage", $"exploded.deviceType", $"exploded.duration", $"exploded.status", $"exploded.timestamp")
.withColumn("explodedBreakdown", explode($"attemptBreakdowns"))
.select($"uniqueId", $"hashID", $"captureDate", $"location", $"kitTag", $"explodedBreakdown.property".alias("liProperty"), $"explodedBreakdown.success".alias("liSuccess"), $"explodedBreakdown.value".alias("liValue"), $"countAverage".alias("liCountAverage"), $"deviceType".alias("liDeviceType"), $"duration".alias("liDuration"), $"status".alias("liStatus"), $"timestamp".alias("liTimestamp"))

val portrait = spark.sql("""select
                   from_unixtime(substring(METRIC_DATA.captureDate.*, 1, 10)) as captureDate, 
                   METRIC_DATA.kitTag,
                   METRIC_DATA.location,
                   METRIC_DATA.uniqueId,
                   _id.`$oid` as hashID,
                   METRIC_DATA.metricBreakdowns.portrait.attempts 
                   from datasql""")
.withColumn("exploded", explode($"attempts"))
.select($"uniqueId", $"hashID", $"captureDate", $"location", $"kitTag", $"exploded.attemptBreakdowns", $"exploded.deviceType", $"exploded.duration", $"exploded.status", $"exploded.timestamp")
.withColumn("explodedBreakdown", explode($"attemptBreakdowns"))
.select($"uniqueId", $"hashID", $"captureDate", $"location", $"kitTag", $"explodedBreakdown.property".alias("ptProperty"), $"explodedBreakdown.success".alias("ptSuccess"), $"explodedBreakdown.value".alias("ptValue"), $"deviceType".alias("ptDeviceType"), $"duration".alias("ptDuration"), $"status".alias("ptStatus"), $"timestamp".alias("ptTimestamp"))

val useCase = spark.sql("""select
                   from_unixtime(substring(METRIC_DATA.captureDate.*, 1, 10)) as captureDate, 
                   METRIC_DATA.kitTag,
                   METRIC_DATA.location,
                   METRIC_DATA.uniqueId,
                   _id.`$oid` as hashID,
                   METRIC_DATA.metricBreakdowns.useCase.attempts 
                   from datasql""")
.withColumn("exploded", explode($"attempts"))
.select($"uniqueId", $"hashID", $"captureDate", $"location", $"kitTag", $"exploded.attemptBreakdowns", $"exploded.duration", $"exploded.status")
.withColumn("explodedBreakdown", explode($"attemptBreakdowns"))
.select($"uniqueId", $"hashID", $"captureDate", $"location", $"kitTag", $"explodedBreakdown.property".alias("ucProperty"), $"explodedBreakdown.success".alias("ucSuccess"), $"explodedBreakdown.value".alias("ucValue"), $"duration".alias("ucDuration"), $"status".alias("ucStatus"))

// COMMAND ----------

val totalMetadata_0 = fingerprints.join(livenessEvents,Seq("uniqueId", "hashID", "captureDate", "location", "kitTag"),joinType="outer")
val totalMetadata_1 = totalMetadata_0.join(portrait,Seq("uniqueId", "hashID", "captureDate", "location", "kitTag"),joinType="outer")
val totalMetadata = totalMetadata_1.join(useCase,Seq("uniqueId", "hashID", "captureDate", "location", "kitTag"),joinType="outer")

// COMMAND ----------

fingerprints.write.json("dbfs:/mnt/S3/mtn_xmlmetadata/fingerprints")
livenessEvents.write.json("dbfs:/mnt/S3/mtn_xmlmetadata/livenessEvents")
portrait.write.json("dbfs:/mnt/S3/mtn_xmlmetadata/portrait")
useCase.write.json("dbfs:/mnt/S3/mtn_xmlmetadata/useCase")
totalMetadata.write.json("dbfs:/mnt/S3/mtn_xmlmetadata/totalMetadata")

// COMMAND ----------


