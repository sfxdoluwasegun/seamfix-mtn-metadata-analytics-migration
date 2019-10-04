#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np


# In[2]:


df1 = pd.read_csv('ppp.csv')
df2 = pd.read_csv('ppp1.csv')
df3= pd.read_csv('ppp2.csv')
df4 = pd.read_csv('ppp3.csv')
df5 = pd.read_csv('ppp4.csv')


# In[3]:


df2.rename(columns = { 'Attempt_0_Total' : 'Attempt_1_Total'}, inplace = True)
df3.rename(columns = { 'Attempt_0_Total' : 'Attempt_2_Total'}, inplace = True)
df4.rename(columns = { 'Attempt_0_Total' : 'Attempt_3_Total'}, inplace = True)
df5.rename(columns = { 'Attempt_0_Total' : 'Attempt_4_Total'}, inplace = True)


# In[4]:


dataframes = [df1, df2, df3, df4, df5]


# In[5]:


DF = pd.merge(df1, df2, how = 'outer')


# In[6]:


DF = pd.merge(DF, df3, how = 'outer')


# In[7]:


DF = pd.merge(DF, df4, how = 'outer')


# In[8]:


DF = pd.merge(DF, df5, how = 'outer')


# In[9]:


DF.head()


# In[10]:


DF.shape


# In[11]:


DF.dtypes


# In[12]:


DF.to_csv('part_3_stats.csv', index = False)


# In[ ]:




