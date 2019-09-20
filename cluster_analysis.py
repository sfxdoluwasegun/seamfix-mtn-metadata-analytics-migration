#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
get_ipython().run_line_magic('matplotlib', 'inline')
import plotly
import cufflinks
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 120)


# In[2]:


df = pd.read_csv('clustered_fp_dataset.csv', low_memory = False)


# In[3]:


df.head()


# In[4]:


df.shape


# # Visualization of the clusters accross major parameters

# In[5]:


plt.rc('font', size = 12)
sns.set_style('whitegrid')
colors = ['forest green', 'burnt orange', 'indigo', 'bright yellow', 'bordeaux']
c_palette = sns.xkcd_palette(colors)
sns.set_palette(c_palette)
sns.palplot(c_palette)


# #plot data with seaborn
# facet = sns.lmplot(data=df, x='x', y='y', hue='KM_Clisters', 
#                    fit_reg=False, legend=True, legend_out=True)

# In[6]:


# I will get back to this.


# # Cluster 0 analysis

# In[7]:


cluster_0 = df[df['KM_Clusters'] == 0]


# In[8]:


cluster_0.describe()


# In[9]:


cluster_0.head()


# In[10]:


#unique states in cluster 0?
cluster_0.METRIC_DATA_location.unique()


# In[11]:


print('Success Rate of fingerprint 0 (first attempt):  ', format(df['fingerprint_0_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 1 (first attempt):  ', format(df['fingerprint_1_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 2 (first attempt):  ', format(df['fingerprint_2_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 3 (first attempt):  ', format(df['fingerprint_3_attempts_0_status'].value_counts()))


# In[12]:


#cluster_0 dataframe
print('Success Rate of fingerprint 0 (first attempt):\n ', format(cluster_0['fingerprint_0_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 1 (first attempt):\n ', format(cluster_0['fingerprint_1_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 2 (first attempt):\n ', format(cluster_0['fingerprint_2_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 3 (first attempt):\n ', format(cluster_0['fingerprint_3_attempts_0_status'].value_counts()))


# In[13]:


cluster_0_unique_kittags = cluster_0.METRIC_DATA_kitTag.unique()


# In[14]:


len(cluster_0_unique_kittags)


# In[15]:


cluster_0['fingerprint_0_type'].unique()


# In[16]:


cluster_0['fingerprint_1_type'].unique()


# In[17]:


cluster_0['fingerprint_2_type'].unique()
#this recorded the most failures. Why?


# In[18]:


cluster_0['fingerprint_3_type'].unique()


# In[19]:


#plot each property field by their corresponding success field. Can we find the particular property that fails most?
cluster_0.groupby(['fingerprint_2_attempts_0_attemptBreakdowns_0_property'])['fingerprint_2_attempts_0_attemptBreakdowns_0_success'].value_counts()


# In[20]:


cluster_0.groupby(['fingerprint_2_attempts_0_attemptBreakdowns_1_property'])['fingerprint_2_attempts_0_attemptBreakdowns_1_success'].value_counts()


# In[21]:


cluster_0.groupby(['fingerprint_2_attempts_0_attemptBreakdowns_2_property'])['fingerprint_2_attempts_0_attemptBreakdowns_2_success'].value_counts()


# In[22]:


#cluster_0.groupby(['fingerprint_2_attempts_0_attemptBreakdowns_3_property'])['fingerprint_2_attempts_0_attemptBreakdowns_3_success'].value_counts()
cluster_0.head()


# In[23]:


cluster_0.fingerprint_2_attempts_0_attemptBreakdowns_0_property.unique()


# In[24]:


cluster_0.isnull().values.sum()


# In[25]:


cluster_0.fingerprint_2_attempts_0_attemptBreakdowns_2_property.value_counts()


# In[26]:


inspect = cluster_0.fingerprint_2_attempts_0_attemptBreakdowns_0_property.tolist()


# In[27]:


for i in range (0, len(inspect)):
    i = inspect[0]
    if i == 'MinNumberBlocks':
        inspect.remove(i)
    elif i == 'MinNFIQ':
        inspect.remove(i)
    elif i == 'MaxNumberPoorRidgeFlow':
        inspect.remove(i)
    elif i == 'MaxNumberLightBlocks':
        inspect.remove(i)
    elif i == 'MinNumberGoodBlocks':
        inspect.remove(i)
    elif i == 'MaxNumberDarkBlocks':
        inspect.remove(i)
    elif i == 'MaxNotPartOfPrint':
        inspect.remove(i)
    elif i == 'MinAFIQ':
        inspect.remove(i)
    else:
        pass
    
print(inspect)


# In[28]:


cluster_0.fingerprint_2_attempts_0_attemptBreakdowns_1_success.value_counts()


# In[29]:


cluster_0.head()


# I have identified that fingerprint 2 in this cluster recorded the most number of first time failures. It also had the highest number of attempts value (21) compared to the other fingerprint types in this cluster (8, 5, 6). Let me compare the failure rate to the finger type provided.

# In[30]:


cluster_0.groupby(['fingerprint_2_type'])['fingerprint_2_attempts_0_attemptBreakdowns_0_success'].value_counts()


# In[31]:


print('Left Index Finger has a failure rate of: ' + format(round(2630 /(10611 + 2630), 3)))


# In[32]:


print('Right Index Finger has a failure rate of: ' + format(round(671 /(2988 + 671), 3)))


# In[33]:


print('Right Thumb has a failure rate of: ' + format(round(61 /(347+ 61), 3)))


# In[34]:


cluster_0.groupby(['fingerprint_2_type', 'fingerprint_2_attempts_0_deviceType'])['fingerprint_2_attempts_0_attemptBreakdowns_2_success'].value_counts()


# 
# # Cluster 1 Analysis

# In[35]:


cluster_1 = df[df['KM_Clusters'] == 1]


# In[36]:


cluster_1.shape


# In[37]:


cluster_0.shape


# In[38]:


cluster_1.describe()


# In[39]:


#cluster_1 dataframe
print('Success Rate of fingerprint 0 (first attempt):\n ', format(cluster_1['fingerprint_0_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 1 (first attempt):\n ', format(cluster_1['fingerprint_1_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 2 (first attempt):\n ', format(cluster_1['fingerprint_2_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 3 (first attempt):\n ', format(cluster_1['fingerprint_3_attempts_0_status'].value_counts()))


# In[40]:


cluster_1_unique_kittags = cluster_1.METRIC_DATA_kitTag.unique()


# In[41]:


len(cluster_1_unique_kittags)


# In[42]:


cluster_1.METRIC_DATA_location.unique()


# In[43]:


print(cluster_1['fingerprint_0_type'].unique())
print(cluster_1['fingerprint_1_type'].unique())
print(cluster_1['fingerprint_2_type'].unique())
print(cluster_1['fingerprint_3_type'].unique())


# In[44]:


cluster_1.groupby(['fingerprint_0_attempts_0_attemptBreakdowns_0_property', 'fingerprint_0_attempts_0_deviceType'])['fingerprint_0_attempts_0_attemptBreakdowns_0_success'].value_counts()


# In[45]:


cluster_0.groupby(['fingerprint_0_attempts_0_attemptBreakdowns_0_property', 'fingerprint_0_attempts_0_deviceType'])['fingerprint_0_attempts_0_attemptBreakdowns_0_success'].value_counts()


# In[46]:


cluster_1.groupby(['fingerprint_1_attempts_0_attemptBreakdowns_0_property'])['fingerprint_1_attempts_0_attemptBreakdowns_0_success'].value_counts()


# In[47]:


cluster_1.groupby(['fingerprint_1_attempts_0_attemptBreakdowns_0_property', 'fingerprint_1_attempts_0_deviceType'])['fingerprint_1_attempts_0_attemptBreakdowns_0_success'].value_counts()


# t1 = cluster_1[(cluster_1.fingerprint_1_attempts_0_attemptBreakdowns_0_success == False)][['fingerprint_1_attempts_0_attemptBreakdowns_0_property',
#                                                                                           'fingerprint_1_attempts_0_deviceType',
#                                                                                           'fingerprint_1_attempts_0_attemptBreakdowns_0_success',
#                                                                                           'fingerprint_1_type']]

# In[48]:


t1 = cluster_1.groupby(['fingerprint_1_attempts_0_attemptBreakdowns_0_property', 'fingerprint_1_attempts_0_deviceType', 'fingerprint_1_attempts_0_attemptBreakdowns_0_success']).size().reset_index(name='counts')


# In[49]:


t1.head()


# import plotly.graph_objs as go
# import plotly.plotly as py
# from IPython.core.interactiveshell import InteractiveShell
# InteractiveShell.ast_node_interactivity = 'all'
# from plotly.offline import iplot
# cufflinks.go_offline()
# cufflinks.set_config_file(world_readable=True, theme='pearl')

# -- Standard plotly imports
# import plotly.plotly as py
# import plotly.graph_objs as go
# from plotly.offline import iplot, init_notebook_mode
# -- Using plotly + cufflinks in offline mode
# import cufflinks
# cufflinks.go_offline(connected=True)
# init_notebook_mode(connected=True)

# In[50]:


sns.pairplot(t1, hue='fingerprint_1_attempts_0_attemptBreakdowns_0_success', height=10.0);


# In[51]:


with sns.axes_style(style='ticks'):
    g = sns.catplot("fingerprint_1_attempts_0_deviceType", "counts", "fingerprint_1_attempts_0_attemptBreakdowns_0_success", data=t1, kind="box")
    g.set_axis_labels('Device Type', "Total Count");


# In[52]:


t1[t1['fingerprint_1_attempts_0_attemptBreakdowns_0_success']==True]


# In[53]:


#An attempt at a better plot
sns.catplot(x = 'fingerprint_1_attempts_0_duration_$numberLong' , y = 'fingerprint_1_attempts_0_deviceType', 
           hue = 'fingerprint_1_attempts_0_attemptBreakdowns_0_success', row = 'fingerprint_1_attempts_0_attemptBreakdowns_0_property',
           data = cluster_1, orient = 'h', height = 3, aspect = 3, palette = 'bright', kind = 'violin', dodge = True, bw = 2)


# In[54]:


#An attempt at a better plot
sns.catplot(x = 'fingerprint_1_attempts_0_duration_$numberLong' , y = 'fingerprint_1_attempts_0_deviceType', 
           hue = 'fingerprint_1_attempts_0_attemptBreakdowns_1_success', row = 'fingerprint_1_attempts_0_attemptBreakdowns_1_property',
           data = cluster_1, orient = 'h', height = 3, aspect = 4, palette = 'bright', kind = 'bar', dodge = True)


# In[ ]:




