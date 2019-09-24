#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
get_ipython().run_line_magic('matplotlib', 'inline')
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


# In[55]:


sns.countplot(x = 'fingerprint_1_attempts_0_attemptBreakdowns_0_property', hue = 'fingerprint_1_attempts_0_attemptBreakdowns_0_success', data = cluster_1, orient = 'h', palette = 'deep', dodge = True)


# In[56]:


ax = sns.catplot('fingerprint_1_attempts_0_attemptBreakdowns_0_property', col = 'fingerprint_1_attempts_0_attemptBreakdowns_1_success',
           data = cluster_1, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[57]:


ax = sns.catplot('fingerprint_1_attempts_0_attemptBreakdowns_0_property', row = 'fingerprint_1_attempts_0_deviceType',
                 data = cluster_1[cluster_1['fingerprint_1_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[58]:


ax = sns.catplot('fingerprint_2_attempts_0_attemptBreakdowns_0_property', row = 'fingerprint_2_attempts_0_deviceType',
                 data = cluster_1[cluster_1['fingerprint_2_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# # Cluster 2 Analysis

# In[59]:


cluster_2 = df[df['KM_Clusters'] == 2]


# In[60]:


cluster_2.head()


# In[61]:


cluster_2.shape


# In[62]:


cluster_2.describe()


# In[63]:


#cluster_2 dataframe
print('Success Rate of fingerprint 0 (first attempt):\n ', format(cluster_2['fingerprint_0_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 1 (first attempt):\n ', format(cluster_2['fingerprint_1_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 2 (first attempt):\n ', format(cluster_2['fingerprint_2_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 3 (first attempt):\n ', format(cluster_2['fingerprint_3_attempts_0_status'].value_counts()))


# In[64]:


print(cluster_2['fingerprint_0_type'].unique())
print(cluster_2['fingerprint_1_type'].unique())
print(cluster_2['fingerprint_2_type'].unique())
print(cluster_2['fingerprint_3_type'].unique())


# In[65]:


cluster_2.METRIC_DATA_location.unique()


# In[66]:


len(cluster_2.METRIC_DATA_location.unique())


# In[67]:


cluster_2_kittags = cluster_2.METRIC_DATA_kitTag.unique()


# In[68]:


cluster_2_kittags


# In[69]:


len(cluster_2_kittags)


# In[70]:


sns.catplot(x = 'fingerprint_0_attempts_0_duration_$numberLong' , y = 'fingerprint_0_attempts_0_deviceType', 
           hue = 'fingerprint_0_attempts_0_attemptBreakdowns_0_success', row = 'fingerprint_0_attempts_0_attemptBreakdowns_0_property',
           data = cluster_2, orient = 'h', height = 3, aspect = 3, palette = 'bright', kind = 'violin', dodge = True, bw = 2)


# In[71]:


sns.catplot(x = 'fingerprint_1_attempts_0_duration_$numberLong' , y = 'fingerprint_1_attempts_0_deviceType', 
           hue = 'fingerprint_1_attempts_0_attemptBreakdowns_0_success', row = 'fingerprint_1_attempts_0_attemptBreakdowns_0_property',
           data = cluster_2, orient = 'h', height = 3, aspect = 3, palette = 'bright', kind = 'violin', dodge = True, bw = 2)


# In[158]:


sns.catplot(x = 'fingerprint_2_attempts_0_duration_$numberLong' , y = 'fingerprint_2_attempts_0_deviceType', 
           hue = 'fingerprint_2_attempts_0_attemptBreakdowns_0_success', row = 'fingerprint_2_attempts_0_attemptBreakdowns_0_property',
           data = cluster_2, orient = 'h', height = 3, aspect = 4, palette = 'bright', kind = 'bar', dodge = True)


# In[73]:


#An attempt at a better plot
sns.catplot(x = 'fingerprint_3_attempts_0_duration_$numberLong' , y = 'fingerprint_3_attempts_0_deviceType', 
           hue = 'fingerprint_3_attempts_0_attemptBreakdowns_0_success', row = 'fingerprint_3_attempts_0_attemptBreakdowns_0_property',
           data = cluster_2, orient = 'h', height = 3, aspect = 4, palette = 'bright', kind = 'bar', dodge = True)


# In[74]:


ax = sns.catplot('fingerprint_0_attempts_0_attemptBreakdowns_0_property', col = 'fingerprint_0_attempts_0_attemptBreakdowns_0_success',
           data = cluster_2, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[75]:


ax = sns.catplot('fingerprint_1_attempts_0_attemptBreakdowns_0_property', col = 'fingerprint_1_attempts_0_attemptBreakdowns_0_success',
           data = cluster_2, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[76]:


ax = sns.catplot('fingerprint_2_attempts_0_attemptBreakdowns_0_property', col = 'fingerprint_2_attempts_0_attemptBreakdowns_0_success',
           data = cluster_2, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[77]:


ax = sns.catplot('fingerprint_3_attempts_0_attemptBreakdowns_0_property', col = 'fingerprint_3_attempts_0_attemptBreakdowns_0_success',
           data = cluster_2, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[78]:


ax = sns.catplot('fingerprint_3_attempts_0_attemptBreakdowns_1_property', col = 'fingerprint_3_attempts_0_attemptBreakdowns_1_success',
           data = cluster_2, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[79]:


ax = sns.catplot('fingerprint_2_attempts_0_attemptBreakdowns_1_property', col = 'fingerprint_2_attempts_0_attemptBreakdowns_1_success',
           data = cluster_2, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[80]:


ax = sns.catplot('fingerprint_1_attempts_0_attemptBreakdowns_1_property', col = 'fingerprint_1_attempts_0_attemptBreakdowns_1_success',
           data = cluster_2, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[81]:


ax = sns.catplot('fingerprint_0_attempts_0_attemptBreakdowns_1_property', col = 'fingerprint_0_attempts_0_attemptBreakdowns_1_success',
           data = cluster_2, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[82]:


cluster_2.groupby(['fingerprint_0_attempts_0_attemptBreakdowns_1_property'])['fingerprint_0_attempts_0_attemptBreakdowns_1_success'].value_counts()


# In[83]:


cluster_2.groupby(['fingerprint_1_attempts_0_attemptBreakdowns_1_property'])['fingerprint_1_attempts_0_attemptBreakdowns_1_success'].value_counts()


# In[84]:


ax = sns.catplot('fingerprint_0_attempts_0_attemptBreakdowns_0_property', row = 'fingerprint_0_attempts_0_deviceType',
                 data = cluster_2[cluster_2['fingerprint_0_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[85]:


ax = sns.catplot('fingerprint_1_attempts_0_attemptBreakdowns_0_property', row = 'fingerprint_1_attempts_0_deviceType',
                 data = cluster_2[cluster_2['fingerprint_1_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[86]:


ax = sns.catplot('fingerprint_2_attempts_0_attemptBreakdowns_0_property', row = 'fingerprint_2_attempts_0_deviceType',
                 data = cluster_2[cluster_2['fingerprint_2_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[87]:


ax = sns.catplot('fingerprint_3_attempts_0_attemptBreakdowns_0_property', row = 'fingerprint_3_attempts_0_deviceType',
                 data = cluster_2[cluster_2['fingerprint_3_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[88]:


ax = sns.catplot('fingerprint_0_attempts_0_attemptBreakdowns_1_property', row = 'fingerprint_0_attempts_0_deviceType',
                 data = cluster_2[cluster_2['fingerprint_0_attempts_0_attemptBreakdowns_1_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[89]:


ax = sns.catplot('fingerprint_1_attempts_0_attemptBreakdowns_1_property', row = 'fingerprint_1_attempts_0_deviceType',
                 data = cluster_2[cluster_2['fingerprint_1_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[90]:


ax = sns.catplot('fingerprint_2_attempts_0_attemptBreakdowns_1_property', row = 'fingerprint_2_attempts_0_deviceType',
                 data = cluster_2[cluster_2['fingerprint_2_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[91]:


ax = sns.catplot('fingerprint_3_attempts_0_attemptBreakdowns_1_property', row = 'fingerprint_3_attempts_0_deviceType',
                 data = cluster_2[cluster_2['fingerprint_3_attempts_0_attemptBreakdowns_1_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# # Cluster 3 Analysis

# In[92]:


cluster_3 = df[df['KM_Clusters'] == 3]


# In[93]:


cluster_3.shape


# In[94]:


cluster_3.head()


# In[95]:


cluster_3.describe()


# In[96]:


print('Success Rate of fingerprint 0 (first attempt):\n ', format(cluster_3['fingerprint_0_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 1 (first attempt):\n ', format(cluster_3['fingerprint_1_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 2 (first attempt):\n ', format(cluster_3['fingerprint_2_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 3 (first attempt):\n ', format(cluster_3['fingerprint_3_attempts_0_status'].value_counts()))


# In[97]:


print(cluster_3['fingerprint_0_type'].unique())
print(cluster_3['fingerprint_1_type'].unique())
print(cluster_3['fingerprint_2_type'].unique())
print(cluster_3['fingerprint_3_type'].unique())


# In[98]:


cluster_3.METRIC_DATA_location.unique()


# In[99]:


len(cluster_3.METRIC_DATA_location.unique())


# In[100]:


cluster_3_kittags = cluster_3.METRIC_DATA_kitTag.unique()


# In[101]:


len(cluster_3_kittags)


# In[102]:


sns.catplot(x = 'fingerprint_0_attempts_0_duration_$numberLong' , y = 'fingerprint_0_attempts_0_deviceType', 
           hue = 'fingerprint_0_attempts_0_attemptBreakdowns_0_success', row = 'fingerprint_0_attempts_0_attemptBreakdowns_0_property',
           data = cluster_3, orient = 'h', height = 3, aspect = 3, palette = 'bright', kind = 'violin', dodge = True, bw = 2)


# In[103]:


sns.catplot(x = 'fingerprint_1_attempts_0_duration_$numberLong' , y = 'fingerprint_1_attempts_0_deviceType', 
           hue = 'fingerprint_1_attempts_0_attemptBreakdowns_0_success', row = 'fingerprint_1_attempts_0_attemptBreakdowns_0_property',
           data = cluster_3, orient = 'h', height = 3, aspect = 3, palette = 'bright', kind = 'violin', dodge = True, bw = 2)


# In[104]:


sns.catplot(x = 'fingerprint_2_attempts_0_duration_$numberLong' , y = 'fingerprint_2_attempts_0_deviceType', 
           hue = 'fingerprint_2_attempts_0_attemptBreakdowns_0_success', row = 'fingerprint_2_attempts_0_attemptBreakdowns_0_property',
           data = cluster_3, orient = 'h', height = 3, aspect = 4, palette = 'bright', kind = 'bar', dodge = True)


# In[105]:


sns.catplot(x = 'fingerprint_3_attempts_0_duration_$numberLong' , y = 'fingerprint_3_attempts_0_deviceType', 
           hue = 'fingerprint_3_attempts_0_attemptBreakdowns_0_success', row = 'fingerprint_3_attempts_0_attemptBreakdowns_0_property',
           data = cluster_3, orient = 'h', height = 3, aspect = 4, palette = 'bright', kind = 'bar', dodge = True)


# In[106]:


ax = sns.catplot('fingerprint_0_attempts_0_attemptBreakdowns_0_property', col = 'fingerprint_0_attempts_0_attemptBreakdowns_0_success',
           data = cluster_3, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[107]:


ax = sns.catplot('fingerprint_1_attempts_0_attemptBreakdowns_0_property', col = 'fingerprint_1_attempts_0_attemptBreakdowns_0_success',
           data = cluster_3, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[108]:


ax = sns.catplot('fingerprint_2_attempts_0_attemptBreakdowns_0_property', col = 'fingerprint_2_attempts_0_attemptBreakdowns_0_success',
           data = cluster_3, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[109]:


ax = sns.catplot('fingerprint_3_attempts_0_attemptBreakdowns_0_property', col = 'fingerprint_3_attempts_0_attemptBreakdowns_0_success',
           data = cluster_3, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[110]:


ax = sns.catplot('fingerprint_3_attempts_0_attemptBreakdowns_1_property', col = 'fingerprint_3_attempts_0_attemptBreakdowns_1_success',
           data = cluster_3, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[111]:


ax = sns.catplot('fingerprint_2_attempts_0_attemptBreakdowns_1_property', col = 'fingerprint_2_attempts_0_attemptBreakdowns_1_success',
           data = cluster_3, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[112]:


ax = sns.catplot('fingerprint_1_attempts_0_attemptBreakdowns_1_property', col = 'fingerprint_1_attempts_0_attemptBreakdowns_1_success',
           data = cluster_3, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[113]:


ax = sns.catplot('fingerprint_0_attempts_0_attemptBreakdowns_1_property', col = 'fingerprint_0_attempts_0_attemptBreakdowns_1_success',
           data = cluster_3, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[114]:


cluster_3.groupby(['fingerprint_0_attempts_0_attemptBreakdowns_1_property'])['fingerprint_0_attempts_0_attemptBreakdowns_1_success'].value_counts()


# In[115]:


cluster_3.groupby(['fingerprint_1_attempts_0_attemptBreakdowns_1_property'])['fingerprint_1_attempts_0_attemptBreakdowns_1_success'].value_counts()


# In[116]:


ax = sns.catplot('fingerprint_0_attempts_0_attemptBreakdowns_0_property', row = 'fingerprint_0_attempts_0_deviceType',
                 data = cluster_3[cluster_3['fingerprint_0_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[117]:


ax = sns.catplot('fingerprint_1_attempts_0_attemptBreakdowns_0_property', row = 'fingerprint_1_attempts_0_deviceType',
                 data = cluster_3[cluster_3['fingerprint_1_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[118]:


ax = sns.catplot('fingerprint_2_attempts_0_attemptBreakdowns_0_property', row = 'fingerprint_2_attempts_0_deviceType',
                 data = cluster_3[cluster_3['fingerprint_2_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[119]:


ax = sns.catplot('fingerprint_3_attempts_0_attemptBreakdowns_0_property', row = 'fingerprint_3_attempts_0_deviceType',
                 data = cluster_3[cluster_3['fingerprint_3_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[120]:


ax = sns.catplot('fingerprint_0_attempts_0_attemptBreakdowns_1_property', row = 'fingerprint_0_attempts_0_deviceType',
                 data = cluster_3[cluster_3['fingerprint_0_attempts_0_attemptBreakdowns_1_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[121]:


ax = sns.catplot('fingerprint_1_attempts_0_attemptBreakdowns_1_property', row = 'fingerprint_1_attempts_0_deviceType',
                 data = cluster_3[cluster_3['fingerprint_1_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[122]:


ax = sns.catplot('fingerprint_2_attempts_0_attemptBreakdowns_1_property', row = 'fingerprint_2_attempts_0_deviceType',
                 data = cluster_3[cluster_3['fingerprint_2_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[123]:


ax = sns.catplot('fingerprint_3_attempts_0_attemptBreakdowns_1_property', row = 'fingerprint_3_attempts_0_deviceType',
                 data = cluster_3[cluster_3['fingerprint_3_attempts_0_attemptBreakdowns_1_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# # Cluster 4 Analysis

# In[124]:


cluster_4 = df[df['KM_Clusters'] == 4]


# In[125]:


cluster_4.shape


# In[126]:


cluster_4.describe()


# In[127]:


cluster_4.head()


# In[128]:


print('Success Rate of fingerprint 0 (first attempt):\n ', format(cluster_4['fingerprint_0_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 1 (first attempt):\n ', format(cluster_4['fingerprint_1_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 2 (first attempt):\n ', format(cluster_4['fingerprint_2_attempts_0_status'].value_counts()))
print('Success Rate of fingerprint 3 (first attempt):\n ', format(cluster_4['fingerprint_3_attempts_0_status'].value_counts()))


# In[129]:


print(cluster_4['fingerprint_0_type'].unique())
print(cluster_4['fingerprint_1_type'].unique())
print(cluster_4['fingerprint_2_type'].unique())
print(cluster_4['fingerprint_3_type'].unique())


# In[130]:


cluster_4.METRIC_DATA_location.unique()


# In[131]:


len(cluster_4.METRIC_DATA_location.unique())


# In[132]:


len(df.METRIC_DATA_location.unique())


# In[133]:


cluster_4_kittags = cluster_4.METRIC_DATA_kitTag.unique()


# In[134]:


len(cluster_4_kittags)


# In[135]:


sns.catplot(x = 'fingerprint_0_attempts_0_duration_$numberLong' , y = 'fingerprint_0_attempts_0_deviceType', 
           hue = 'fingerprint_0_attempts_0_attemptBreakdowns_0_success', row = 'fingerprint_0_attempts_0_attemptBreakdowns_0_property',
           data = cluster_4, orient = 'h', height = 3, aspect = 3, palette = 'bright', kind = 'violin', dodge = True, bw = 2)


# In[136]:


sns.catplot(x = 'fingerprint_1_attempts_0_duration_$numberLong' , y = 'fingerprint_1_attempts_0_deviceType', 
           hue = 'fingerprint_1_attempts_0_attemptBreakdowns_0_success', row = 'fingerprint_1_attempts_0_attemptBreakdowns_0_property',
           data = cluster_4, orient = 'h', height = 3, aspect = 3, palette = 'bright', kind = 'violin', dodge = True, bw = 2)


# In[137]:


sns.catplot(x = 'fingerprint_2_attempts_0_duration_$numberLong' , y = 'fingerprint_2_attempts_0_deviceType', 
           hue = 'fingerprint_2_attempts_0_attemptBreakdowns_0_success', row = 'fingerprint_2_attempts_0_attemptBreakdowns_0_property',
           data = cluster_4, orient = 'h', height = 3, aspect = 4, palette = 'bright', kind = 'bar', dodge = True)


# In[138]:


sns.catplot(x = 'fingerprint_2_attempts_0_duration_$numberLong' , y = 'fingerprint_2_attempts_0_deviceType', 
           hue = 'fingerprint_2_attempts_0_attemptBreakdowns_0_success', row = 'fingerprint_2_attempts_0_attemptBreakdowns_0_property',
           data = cluster_4, orient = 'h', height = 3, aspect = 4, palette = 'bright', kind = 'bar', dodge = True)


# In[139]:


ax = sns.catplot('fingerprint_0_attempts_0_attemptBreakdowns_0_property', col = 'fingerprint_0_attempts_0_attemptBreakdowns_0_success',
           data = cluster_4, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[140]:


ax = sns.catplot('fingerprint_1_attempts_0_attemptBreakdowns_0_property', col = 'fingerprint_1_attempts_0_attemptBreakdowns_0_success',
           data = cluster_4, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[141]:


ax = sns.catplot('fingerprint_2_attempts_0_attemptBreakdowns_0_property', col = 'fingerprint_2_attempts_0_attemptBreakdowns_0_success',
           data = cluster_4, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[142]:


ax = sns.catplot('fingerprint_3_attempts_0_attemptBreakdowns_0_property', col = 'fingerprint_3_attempts_0_attemptBreakdowns_0_success',
           data = cluster_4, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[143]:


ax = sns.catplot('fingerprint_3_attempts_0_attemptBreakdowns_1_property', col = 'fingerprint_3_attempts_0_attemptBreakdowns_1_success',
           data = cluster_4, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[144]:


ax = sns.catplot('fingerprint_2_attempts_0_attemptBreakdowns_1_property', col = 'fingerprint_2_attempts_0_attemptBreakdowns_1_success',
           data = cluster_4, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[145]:


ax = sns.catplot('fingerprint_1_attempts_0_attemptBreakdowns_1_property', col = 'fingerprint_1_attempts_0_attemptBreakdowns_1_success',
           data = cluster_4, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[146]:


ax = sns.catplot('fingerprint_0_attempts_0_attemptBreakdowns_1_property', col = 'fingerprint_0_attempts_0_attemptBreakdowns_1_success',
           data = cluster_4, height = 6, aspect = 1, palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[147]:


cluster_4.groupby(['fingerprint_0_attempts_0_attemptBreakdowns_1_property'])['fingerprint_0_attempts_0_attemptBreakdowns_1_success'].count()


# In[148]:


cluster_4.groupby(['fingerprint_1_attempts_0_attemptBreakdowns_1_property'])['fingerprint_1_attempts_0_attemptBreakdowns_1_success'].count()


# In[149]:


ax = sns.catplot('fingerprint_0_attempts_0_attemptBreakdowns_0_property', row = 'fingerprint_0_attempts_0_deviceType',
                 data = cluster_4[cluster_4['fingerprint_0_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[150]:


ax = sns.catplot('fingerprint_1_attempts_0_attemptBreakdowns_0_property', row = 'fingerprint_1_attempts_0_deviceType',
                 data = cluster_4[cluster_4['fingerprint_1_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[151]:


ax = sns.catplot('fingerprint_2_attempts_0_attemptBreakdowns_0_property', row = 'fingerprint_2_attempts_0_deviceType',
                 data = cluster_4[cluster_4['fingerprint_2_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[152]:


ax = sns.catplot('fingerprint_3_attempts_0_attemptBreakdowns_0_property', row = 'fingerprint_3_attempts_0_deviceType',
                 data = cluster_4[cluster_4['fingerprint_3_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[153]:


ax = sns.catplot('fingerprint_0_attempts_0_attemptBreakdowns_1_property', row = 'fingerprint_0_attempts_0_deviceType',
                 data = cluster_4[cluster_4['fingerprint_0_attempts_0_attemptBreakdowns_1_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[154]:


ax = sns.catplot('fingerprint_1_attempts_0_attemptBreakdowns_1_property', row = 'fingerprint_1_attempts_0_deviceType',
                 data = cluster_4[cluster_4['fingerprint_1_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[155]:


ax = sns.catplot('fingerprint_2_attempts_0_attemptBreakdowns_1_property', row = 'fingerprint_2_attempts_0_deviceType',
                 data = cluster_4[cluster_4['fingerprint_2_attempts_0_attemptBreakdowns_0_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[156]:


ax = sns.catplot('fingerprint_3_attempts_0_attemptBreakdowns_1_property', row = 'fingerprint_3_attempts_0_deviceType',
                 data = cluster_4[cluster_4['fingerprint_3_attempts_0_attemptBreakdowns_1_success'] == False], height = 4, aspect = 1, 
                 palette = 'deep', kind = 'count', dodge = False)
ax.set_xticklabels( rotation= 45, ha="right", fontsize = 10)
plt.tight_layout()
plt.show()


# In[ ]:




