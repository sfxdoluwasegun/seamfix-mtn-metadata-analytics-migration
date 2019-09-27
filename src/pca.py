import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql.functions import *

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

# COMMAND ----------

data_random = create_2D_gaussian(mn=50, variance=1, cov=0, n=100)
data_correlated = create_2D_gaussian(mn=50, variance=1, cov=.9, n=100)

# COMMAND ----------

establish_conn()
pt_path_en = "s3://seamfix-machine-learning/mtn_xmlmetadata/portrait_encoded/*.json"
pt_path_re = "s3://seamfix-machine-learning/mtn_xmlmetadata/portrait_refined/*.json"

data_en = spark.read.json(pt_path_en)
data_re = spark.read.json(pt_path_re)

# COMMAND ----------

data_en1, data_en2 = data_en.randomSplit([0.1, 0.9])

# COMMAND ----------

data_en1.count()

# COMMAND ----------

data_en2.count()

# COMMAND ----------

display(data_en1)

# COMMAND ----------

# generate layout and plot data
fig, ax = prepare_plot(np.arange(46, 55, 2), np.arange(46, 55, 2))
ax.set_xlabel(r'Simulated $x_1$ values'), ax.set_ylabel(r'Simulated $x_2$ values')
ax.set_xlim(45, 54.5), ax.set_ylim(45, 54.5)
plt.scatter(data_random[:,0], data_random[:,1], s=14**2, c='#d6ebf2', edgecolors='#8cbfd0', alpha=0.75)
display(fig)

# COMMAND ----------


# generate layout and plot data
fig, ax = prepare_plot(np.arange(46, 55, 2), np.arange(46, 55, 2))
ax.set_xlabel(r'Simulated $x_1$ values'), ax.set_ylabel(r'Simulated $x_2$ values')
ax.set_xlim(45.5, 54.5), ax.set_ylim(45.5, 54.5)
plt.scatter(data_correlated[:,0], data_correlated[:,1], s=14**2, c='#d6ebf2',
            edgecolors='#8cbfd0', alpha=0.75)
display(fig)

# COMMAND ----------

# array = np.array(dataPT.select("ptDuration","ptValue").collect())
array = data_en1.select("ptDuration","ptValue").toPandas().values

# COMMAND ----------

print "maximum values: %5.3f"% array[:,0].max(), array[:,1].max()
print "minimun values: %5.3f"% array[:,0].min(), array[:,1].min()

# COMMAND ----------

array_norm = (array - array.mean(axis=0)) / array.std(axis=0)
# array_norm = array / np.linalg.norm(array)


# COMMAND ----------

print "maximum values: %5.3f"% array_norm[:,0].max(), array_norm[:,1].max()
print "minimun values: %5.3f"% array_norm[:,0].min(), array_norm[:,1].min()

# COMMAND ----------

# generate layout and plot data_en1
fig, ax = prepare_plot(np.arange(82.789, 9.29717174523, 1), np.arange(-0.560, -0.935171696381, 1))
ax.set_xlabel(r'Simulated $x_1$ values'), ax.set_ylabel(r'Simulated $x_2$ values')
ax.set_xlim(81.789, 8.29717174523), ax.set_ylim(-0.460, -0.835171696381)
plt.scatter(array_norm[:,0], array_norm[:,1], s=14**2, c='#d6ebf2',
            edgecolors='#8cbfd0', alpha=0.75)
display(fig)



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

