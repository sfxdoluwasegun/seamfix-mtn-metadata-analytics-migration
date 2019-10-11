#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
import copy
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')


# Since my system cannot fit all excel files in memeory, this is an interative notebook.
# All you need do is kill the kernel after running this first batch and then load portrait_2.csv next.
# Again after that, kill the kernel and load portrait_3.csv

# In[3]:


main = pd.read_csv('portrait_1.csv', low_memory = False)


# In[4]:


pd.set_option('display.max_columns', 20000)
pd.set_option('display.max_rows', 218)


# In[7]:


att0 = main.columns[main.columns.str.contains(pat = 'METRIC_DATA_metricBreakdowns_portrait_attempts_0_')]
att1 = main.columns[main.columns.str.contains(pat = 'METRIC_DATA_metricBreakdowns_portrait_attempts_1_')]
att2 = main.columns[main.columns.str.contains(pat = 'METRIC_DATA_metricBreakdowns_portrait_attempts_2_')]
att3 = main.columns[main.columns.str.contains(pat = 'METRIC_DATA_metricBreakdowns_portrait_attempts_3_')]
att4 = main.columns[main.columns.str.contains(pat = 'METRIC_DATA_metricBreakdowns_portrait_attempts_4_')]


# In[8]:


df1 = main[att1]
df0 = main[att0]
df2 = main[att2]
df3 = main[att3]
df4 = main[att4]


# In[14]:


column_names_0 = []
cols0 = df0.columns.tolist()
for col in cols0:
    pattern = "METRIC_DATA_metricBreakdowns_portrait_"
    if pattern in col:
        column_names_0.append(col[49:])
    else:
        column_names_0.append(col)
        
column_names_1 = []
cols1 = df1.columns.tolist()
for col in cols1:
    pattern = "METRIC_DATA_metricBreakdowns_portrait_"
    if pattern in col:
        column_names_1.append(col[49:])
    else:
        column_names_1.append(col)
        
column_names_2 = []
cols2 = df2.columns.tolist()
for col in cols2:
    pattern = "METRIC_DATA_metricBreakdowns_portrait_"
    if pattern in col:
        column_names_2.append(col[49:])
    else:
        column_names_2.append(col)
        
        
column_names_3 = []
cols3 = df3.columns.tolist()
for col in cols3:
    pattern = "METRIC_DATA_metricBreakdowns_portrait_"
    if pattern in col:
        column_names_3.append(col[49:])
    else:
        column_names_3.append(col)
        
        
column_names_4 = []
cols4 = df4.columns.tolist()
for col in cols4:
    pattern = "METRIC_DATA_metricBreakdowns_portrait_"
    if pattern in col:
        column_names_4.append(col[49:])
    else:
        column_names_4.append(col)


# In[16]:


df0.set_axis(column_names_0, axis = 1, inplace = True)
df1.set_axis(column_names_1, axis = 1, inplace = True)
df2.set_axis(column_names_2, axis = 1, inplace = True)
df3.set_axis(column_names_3, axis = 1, inplace = True)
df4.set_axis(column_names_4, axis = 1, inplace = True)


# In[17]:


total = [df1, df2, df3, df4, df0]
final = pd.concat(total, axis = 0, sort = True)


# In[18]:


final.shape


# In[19]:


final.status.value_counts()


# In[28]:


len(final.attemptBreakdowns_7_property.unique())


# In[29]:


ID = list(range(1, 91))


# In[30]:


properties = pd.DataFrame.from_dict(ID)


# In[31]:


properties.rename(columns = { 0 : 'ID'}, inplace = True)


# In[32]:


property_cols = []
for col in list(final.columns):
    if 'property' in col:
        property_cols.append(col)
    else:
        pass


# In[33]:


#property_cols


# In[34]:


fina_false = final[final['status']==False]


# In[35]:


len(fina_false.attemptBreakdowns_0_property.unique())


# In[36]:


prop = list(final.attemptBreakdowns_0_property.unique())


# In[37]:


properties['index'] = prop


# In[38]:


fina_false.attemptBreakdowns_0_property.value_counts().reset_index(name = 'count' + format(property_cols.index('attemptBreakdowns_0_property')))


# In[39]:


property_cols.index('attemptBreakdowns_0_property')


# In[40]:


for col in property_cols:
    blue = fina_false[col].value_counts().reset_index(name = 'count' + format(property_cols.index(col)))
    properties = pd.merge(properties, blue, how = 'left')


# In[41]:


properties.head()


# In[42]:


properties.fillna(0, inplace = True)


# In[43]:


properties['index'].to_list()


# In[44]:


count_cols = []
for col in properties.columns:
    if 'count' in col:
        count_cols.append(col)
    else:
        pass


# In[46]:


def coerce_df_columns_to_numeric(df, column_list):
    df[column_list] = df[column_list].apply(pd.to_numeric, errors='coerce')
    
coerce_df_columns_to_numeric(properties, count_cols)


# At this point, if you have run a previous notebook, please rename the new column name (seen in the next cell) to part_2_total or part_3_total depending on what part you are working with

# In[49]:


properties['part_1_total'] = properties[count_cols].sum(axis=1)


# In[48]:


#properties.drop('Attempt_0_Total', inplace = True, axis = 1)


# Don't forget that if you are working on portrait_2.csv or portrait_3.csv you need to change the name of the column part_1_total to fit.

# In[50]:


Total_DF = properties[['index', 'part_1_total']]


# If you are running the code below for portrait_2.csv or portrait_3.csv, please be mindful that this will overwrite what you have saved in the previous (iteration). Name of csv files must be unique in every directory so please rename it.

# In[51]:


Total_DF.to_csv('part1X5.csv', index = False)


# In[ ]:




