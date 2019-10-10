#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd


# In[2]:


df1 = pd.read_csv('stats_part1.csv')
df2 = pd.read_csv('stats_part2.csv')
df3 = pd.read_csv('stats_part3.csv')


# In[5]:


df3.head()


# In[9]:


col1 = ('index','maxi1' , 'minim1', 'count1', 'total1')
col2 = ('index', 'maxi2', 'minim2', 'count2', 'total2')


# In[11]:


df2.rename(columns = {'maxi':'maxi1' , 'minim':'minim1', 'count':'count1', 'total':'total1'}, inplace = True)
df3.rename(columns = {'maxi':'maxi2' , 'minim':'minim2', 'count':'count2', 'total':'total2'}, inplace = True)


# In[16]:


dataframes = [df1, df2, df3]
df = pd.merge(df1, df2, how = 'inner', on = 'index')


# In[18]:


df = pd.merge(df, df3, how = 'inner', on = 'index')


# In[19]:


df.shape


# In[20]:


df.head()


# In[21]:


df.describe()


# In[28]:


conditions = [
    (df['minim'] <= df['minim1']) & (df['minim'] <= df['minim2']), 
    df['minim1'] <= df['minim2'],
    df['minim2'] < df['minim1']]

choices = [df['minim'], df['minim1'], df['minim2']]

df['all_round_min'] = np.select(conditions, choices, default=np.nan)


# In[30]:


conditions1 = [
    (df['maxi'] >= df['maxi1']) & (df['maxi'] >= df['maxi2']), 
    df['maxi1'] >= df['maxi2'],
    df['maxi2'] > df['maxi1']]

choices1 = [df['maxi'], df['maxi1'], df['maxi2']]

df['all_round_max'] = np.select(conditions1 , choices1, default=np.nan)


# In[33]:


df.head()


# In[27]:


#df.drop('all_round_min', axis = 1, inplace = True)


# In[32]:


df['all_round_count'] = df['count'] + df['count1'] + df['count2']
df['all_round_total'] = df['total'] + df['total1'] + df['total2']


# In[34]:


df['all_round_avg'] = df['all_round_total'] / df['all_round_count']


# In[35]:


df.head(20)


# In[36]:


okurr = df[['index', 'all_round_min', 'all_round_max', 'all_round_count', 'all_round_avg']]


# In[37]:


#okurr.to_csv('for_olamide.csv', index = False)


# In[38]:


okurr.head()


# In[ ]:




