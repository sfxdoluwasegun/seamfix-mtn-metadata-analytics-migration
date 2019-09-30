import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame

# COMMAND ----------

def establish_conn():
  ACCESS_KEY = "AKIAU7APHSU5V6VGK7XV"
  SECRET_KEY = "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" 
  sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
  sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", SECRET_KEY)
  
def prepare_plot(xticks, yticks, figsize=(10.5, 6), hide_labels=False, grid_color='#999999',
                 grid_width=1.0):
    """Template for generating the plot layout."""
    plt.close()
    fig, ax = plt.subplots(figsize=figsize, facecolor='white', edgecolor='white')
    ax.axes.tick_params(labelcolor='#999999', labelsize='10')
    for axis, ticks in [(ax.get_xaxis(), xticks), (ax.get_yaxis(), yticks)]:
        axis.set_ticks_position('none')
        axis.set_ticks(ticks)
        axis.label.set_color('#999999')
        if hide_labels: axis.set_ticklabels([])
    plt.grid(color=grid_color, linewidth=grid_width, linestyle='-')
    map(lambda position: ax.spines[position].set_visible(False), ['bottom', 'top', 'left', 'right'])
    return fig, ax

def create_2D_gaussian(mn, variance, cov, n):
    """Randomly sample points from a two-dimensional Gaussian distribution"""
    np.random.seed(142)
    return np.random.multivariate_normal(np.array([mn, mn]), np.array([[variance, cov], [cov, variance]]), n)
  
def join_livenessEvent_dataframe(df1: DataFrame, df2: DataFrame, df3: DataFrame, df4: DataFrame) -> DataFrame:
  df2 = df2.withColumnRenamed('hashID', 'hashID_')
  df1 = df1.join(df2, df1.hashID == df2.hashID_)
  df1 = df1.drop('hashID_')
  df3 = df3.withColumnRenamed('hashID', 'hashID_')
  df = df1.join(df3, df1.hashID == df3.hashID_)
  df = df.drop('hashID_')
  df4 = df4.withColumnRenamed('hashID', 'hashID_')
  df = df.join(df4, df.hashID == df4.hashID_)
  df = df.drop('hashID_')
  return df


def plot(data, x1, x2, startx1, stopx1, stepx1, startx2, stopx2, stepx2):
  # generate layout and plot data
  startx1 = int(startx1)
  startx2 = int(startx2)
  stopx1= int(stopx1+1)
  stopx2= int(stopx2+1)
  fig, ax = prepare_plot(np.arange(startx1, stopx1, stepx1), np.arange(startx2, stopx2, stepx2))
  ax.set_xlabel(r'Simulated $x_1$ %s'%(x1)), ax.set_ylabel(r'Simulated $x_2$ %s'%(x2))
  ax.set_xlim(startx1-0.5, stopx1-0.5), ax.set_ylim(startx2-0.5, stopx2-0.5)
  plt.scatter(data[:,0], data[:,1], s=14**2, c='#d6ebf2', edgecolors='#8cbfd0', alpha=0.75)
  display(fig)
  
def double_cols(df: DataFrame) -> list:
  columns = df.columns
  doubles = ["Duration", "Value", "Attempts", "CountAverage"]
  cols_list = [i for i in columns for j in doubles if j in i]
  return cols_list  

# COMMAND ----------

data_random = create_2D_gaussian(mn=50, variance=1, cov=0, n=100)
data_correlated = create_2D_gaussian(mn=50, variance=1, cov=.9, n=100)

# COMMAND ----------

establish_conn()
pt_path = "s3://seamfix-machine-learning/mtn_xmlmetadata/portrait_encoded/*.json"
fp_path = "s3://seamfix-machine-learning/mtn_xmlmetadata/fingerprint_encoded/*.json"
uc_path = "s3://seamfix-machine-learning/mtn_xmlmetadata/useCase_encoded/*.json"

li_path1 = "s3://seamfix-machine-learning/mtn_xmlmetadata/livenessEvents_encoded/part1/*.json"
li_path2 = "s3://seamfix-machine-learning/mtn_xmlmetadata/livenessEvents_encoded/part2/*.json"
li_path3 = "s3://seamfix-machine-learning/mtn_xmlmetadata/livenessEvents_encoded/part3/*.json"
li_path4 = "s3://seamfix-machine-learning/mtn_xmlmetadata/livenessEvents_encoded/part4/*.json"

li_df1 = spark.read.json(li_path1)
li_df2 = spark.read.json(li_path2)
li_df3 = spark.read.json(li_path3)
li_df4 = spark.read.json(li_path4)

pt_df = spark.read.json(pt_path)
fp_df = spark.read.json(fp_path)
uc_df = spark.read.json(uc_path)
li_df = join_livenessEvent_dataframe(li_df1, li_df2, li_df3, li_df4)


# COMMAND ----------

pt_split, _ = pt_df.randomSplit([0.1, 0.9])
fp_split, _ = fp_df.randomSplit([0.1, 0.9])
uc_split, _ = uc_df.randomSplit([0.1, 0.9])
li_split, _ = li_df.randomSplit([0.1, 0.9])

# COMMAND ----------

display(pt_split)

# COMMAND ----------

display(fp_split)

# COMMAND ----------

display(uc_split)

# COMMAND ----------

display(li_df2)

# COMMAND ----------

plot(data_random, "data_randomX1", "data_randomX2", data_random[:,0].min(), data_random[:,0].max(), 2, data_random[:,1].min(), data_random[:,1].max(), 2)

# COMMAND ----------

plot(data_correlated, "data_correlatedX1", "data_correlatedX2", data_correlated[:,0].min(), data_correlated[:,0].max(), 2, data_correlated[:,1].min(), data_correlated[:,1].max(), 2)

# COMMAND ----------

# array = np.array(dataPT.select("ptDuration","ptValue").collect())
data_pt = pt_split.select(double_cols(pt_split)).toPandas().values

# COMMAND ----------

data_uc = uc_split.select(double_cols(uc_split)).toPandas().values

# COMMAND ----------

data_fp = fp_split.select(double_cols(fp_split)).toPandas().values

# COMMAND ----------

data_li = li_split.select(double_cols(li_split)).toPandas().values

# COMMAND ----------

print "maximum values: %5.3f"% array[:,0].max(), array[:,1].max()
print "minimun values: %5.3f"% array[:,0].min(), array[:,1].min()

# COMMAND ----------

array_norm = (array - array.mean(axis=0)) / array.std(axis=0)
# array_norm = array / np.linalg.norm(array)


# COMMAND ----------

print "maximum values: %5.3f"% array_norm[:,0].max(), array_norm[:,1].max()
print "minimun values: %5.3f"% array_norm[:,0].min(), array_norm[:,1].min()


# TODO: Replace <FILL IN> with appropriate code
correlated_data = sc.parallelize(data_correlated)


# COMMAND ----------

# TODO: Replace <FILL IN> with appropriate code
norm_array = sc.parallelize(array_norm)

# COMMAND ----------

#mean_correlated = <FILL IN>
mean_correlated = correlated_data.mean()
#correlated_data_zero_mean = correlated_data.<FILL IN>
correlated_data_zero_mean = correlated_data.map(lambda x: x-mean_correlated)

print mean_correlated
print correlated_data.take(1)
print correlated_data_zero_mean.take(1)

# COMMAND ----------

#mean_correlated = <FILL IN>
mean_norm = norm_array.mean()
#norm_array_zero_mean = norm_array.<FILL IN>
norm_array_zero_mean = norm_array.map(lambda x: x-mean_norm)

print mean_norm
print norm_array.take(1)
print norm_array_zero_mean.take(1)

# COMMAND ----------

# TEST Interpreting PCA (1a)
from databricks_test_helper import Test


Test.assertTrue(np.allclose(mean_correlated, [49.95739037, 49.97180477]),
                'incorrect value for mean_correlated')
Test.assertTrue(np.allclose(correlated_data_zero_mean.take(1)[0], [-0.28561917, 0.10351492]),
                'incorrect value for correlated_data_zero_mean')

# COMMAND ----------

# TEST Interpreting PCA (1a)
from databricks_test_helper import Test


Test.assertTrue(np.allclose(mean_norm, [2.27645176e-17, 6.47784434e-17]),
                'incorrect value for mean_norm')
Test.assertTrue(np.allclose(norm_array_zero_mean.take(1)[0], [-0.20183869, -0.43903477]),
                'incorrect value for norm_array_zero_mean')


# TODO: Replace <FILL IN> with appropriate code
# Compute the covariance matrix using outer products and correlated_data_zero_mean
#correlated_cov = <FILL IN>
num_data_points = correlated_data_zero_mean.count()
print num_data_points
correlated_cov = correlated_data_zero_mean.map(lambda x: np.outer(x, x)).sum()/num_data_points  
print correlated_cov

#Calculate correlated_cov by using correlated_data_zero_mean and applying a map with lambda and numpy outer then summing and finally dividing by #num_data_points

# COMMAND ----------

# TODO: Replace <FILL IN> with appropriate code
# Compute the covariance matrix using outer products and norm_data_zero_mean
#norm_cov = <FILL IN>
num_data_points = norm_array_zero_mean.count()
print num_data_points
norm_cov = norm_array_zero_mean.map(lambda x: np.outer(x, x)).sum()/num_data_points  
print norm_cov

#Calculate norm_cov by using norm_array_zero_mean and applying a map with lambda and numpy outer then summing and finally dividing by #num_data_points

# COMMAND ----------

# TEST Sample covariance matrix (1b)
cov_result = [[ 0.99558386,  0.90148989], [0.90148989, 1.08607497]]
Test.assertTrue(np.allclose(cov_result, correlated_cov), 'incorrect value for correlated_cov')

# COMMAND ----------

# TEST Sample covariance matrix (1b)
cov_result = [[   1.00000000e+00, -6.47404710e-04], [-6.47404710e-04, 1.00000000e+00]]
Test.assertTrue(np.allclose(cov_result, norm_cov), 'incorrect value for norm_cov')

# COMMAND ----------

# MAGIC %md
# MAGIC ### (1c) Covariance Function
# MAGIC 
# MAGIC Next, use the expressions above to write a function to compute the sample covariance matrix for an arbitrary `data` RDD.

# COMMAND ----------

# TODO: Replace <FILL IN> with appropriate code
def estimate_covariance(data):
    """Compute the covariance matrix for a given rdd.

    Note:
        The multi-dimensional covariance array should be calculated using outer products.  Don't
        forget to normalize the data by first subtracting the mean.

    Args:
        data (RDD of np.ndarray):  An `RDD` consisting of NumPy arrays.

    Returns:
        np.ndarray: A multi-dimensional array where the number of rows and columns both equal the
            length of the arrays in the input `RDD`.
    """
    #<FILL IN>
    data_mean = data.mean()
    corr_cov_data_mean_zero = data.map(lambda x: x-data_mean)
    num_data_pts = corr_cov_data_mean_zero.count()
    return corr_cov_data_mean_zero.map(lambda x: np.outer(x, x)).sum()/num_data_pts

# COMMAND ----------

correlated_cov_auto= estimate_covariance(correlated_data)
print correlated_cov_auto

# COMMAND ----------

correlated_cov_auto= estimate_covariance(norm_array)
print correlated_cov_auto

