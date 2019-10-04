#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import copy
import seaborn as sns
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')


# In[2]:


df1 = pd.read_csv('part_1_stats.csv')
df2 = pd.read_csv('part_2_stats.csv')
df3 = pd.read_csv('part_3_stats.csv')


# In[3]:


df1.head()


# In[4]:


df1.columns.to_list()


# In[5]:


cols = [ 'Attempt_0_Total','Attempt_1_Total','Attempt_2_Total','Attempt_3_Total','Attempt_4_Total']


# In[6]:


df1['part_1_Total'] = df1[cols].sum(axis=1)
df2['part_2_Total'] = df2[cols].sum(axis=1)
df3['part_3_Total'] = df3[cols].sum(axis=1)


# In[7]:


df1.head()


# In[8]:


p1 = df1[['index','part_1_Total']]
p2 = df2[['index','part_2_Total']]
p3 = df3[['index','part_3_Total']]


# In[ ]:





# In[10]:


DF = pd.merge(p1, p2, how = 'outer')
DF = pd.merge(DF, p3, how = 'outer')


# In[11]:


DF.head()


# In[12]:


DF['Sum'] = DF.sum(axis=1)


# In[13]:


DF.head()


# In[14]:


DF.to_csv('hard_factsx5.csv', index = False)


# In[ ]:




