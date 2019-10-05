#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import copy
import seaborn as sns
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')


# In[2]:


df1 = pd.read_csv('part1X5.csv')
df2 = pd.read_csv('part2X5.csv')
df3 = pd.read_csv('part3X5.csv')


# In[3]:


df1.head()


# In[4]:


df2.rename(columns = {'total': 'total_2'}, inplace = True)
df3.rename(columns = {'total': 'total_3'}, inplace = True)


# In[5]:


test = pd.merge(df1, df2, how = 'outer', on = 'index')


# In[11]:


fact = pd.merge(test, df3, how = 'outer', on = 'index')


# In[10]:


test.shape


# In[12]:


fact.shape


# In[13]:


fact.dtypes


# In[14]:


fact['consolidated_sum'] = fact.sum(axis=1)


# In[15]:


fact.head()


# In[17]:


fact.to_csv('hard_facts_x5_2222.csv', index = False)


# In[ ]:




