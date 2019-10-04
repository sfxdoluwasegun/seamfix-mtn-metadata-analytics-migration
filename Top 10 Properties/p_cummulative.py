#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import copy
import numpy as np
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
import seaborn as sns


# In[2]:


part2 = pd.read_csv('portrait_1.csv', low_memory = False)


# In[3]:


part2.shape


# In[4]:


pd.set_option('display.max_columns', 20000)
pd.set_option('display.max_rows', 218)


# In[5]:


att1_c = part2.columns[part2.columns.str.contains(pat = 'METRIC_DATA_metricBreakdowns_portrait_attempts_4_')]
list(att1_c)


# In[6]:


len(att1_c)


# In[7]:


att1 = part2[att1_c]


# In[8]:


att1_succ_col = att1.columns[att1.columns.str.contains(pat = 'status')]
att1_succ_col.tolist()


# In[9]:


att1.METRIC_DATA_metricBreakdowns_portrait_attempts_4_status.value_counts()


# In[10]:


att1_c


# In[11]:


column_names = []
cols = att1.columns.tolist()
for col in cols:
    pattern = "METRIC_DATA_metricBreakdowns_portrait_"
    if pattern in col:
        column_names.append(col[49:])
    else:
        column_names.append(col)


# In[12]:


column_names


# In[13]:


att1.set_axis(column_names, axis = 1, inplace = True)


# In[14]:


att1[['attemptBreakdowns_70_property','attemptBreakdowns_70_value','attemptBreakdowns_70_success']]


# In[15]:


len(att1.attemptBreakdowns_0_property.unique())


# In[16]:


att1_false = att1[att1['status']==False]


# In[17]:


att1_false.shape


# In[18]:


att1.attemptBreakdowns_0_property.unique()


# In[19]:


att1_false.head()


# In[20]:


ID = list(range(1, 86))


# In[21]:


properties = pd.DataFrame.from_dict(ID)


# In[22]:


properties.rename(columns = { 0 : 'ID'}, inplace = True)


# In[23]:


properties.head()


# In[24]:


property_cols = []
for col in column_names:
    if 'property' in col:
        property_cols.append(col)
    else:
        pass


# In[25]:


len(property_cols)


# In[26]:


att1_false.attemptBreakdowns_0_property.value_counts().sort_index()


# In[27]:


att1_false.groupby(['attemptBreakdowns_0_property'])['status'].value_counts()


# In[28]:


prop = list(att1.attemptBreakdowns_0_property.unique())


# In[29]:


properties['index'] = prop


# In[30]:


properties.head()


# In[31]:


#properties.drop('property', inplace = True, axis = 1)


# In[32]:


att1_false.attemptBreakdowns_0_property.value_counts().reset_index(name = 'count' + format(property_cols.index('attemptBreakdowns_0_property')))
#att0_false.groupby(['attemptBreakdowns_0_property'])['status'].value_counts().reset_index( name ='count' + format(property_cols.index('attemptBreakdowns_0_property')))


# In[33]:


property_cols.index('attemptBreakdowns_0_property')


# In[34]:


for col in property_cols:
    blue = att1_false[col].value_counts().reset_index(name = 'count' + format(property_cols.index(col)))
    properties = pd.merge(properties, blue, how = 'left')


# In[35]:


properties.head()


# In[36]:


properties.fillna(0, inplace = True)


# In[37]:


#properties.loc[8: 'index'] = 'Empty'


# In[38]:


properties['index'].to_list()


# In[39]:


prop


# In[40]:


count_cols = []
for col in properties.columns:
    if 'count' in col:
        count_cols.append(col)
    else:
        pass


# In[41]:


def coerce_df_columns_to_numeric(df, column_list):
    df[column_list] = df[column_list].apply(pd.to_numeric, errors='coerce')
    
coerce_df_columns_to_numeric(properties, count_cols)


# In[42]:


properties['Attempt_0_Total'] = properties[count_cols].sum(axis=1)


# In[43]:


Total_DF = properties[['index', 'Attempt_0_Total']]


# In[44]:


Total_DF.to_csv('ppp4.csv', index = False)


# In[ ]:





# In[ ]:




