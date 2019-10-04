#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from flatten_json import flatten
import time
import json


# In[2]:


data = []
with open('bfc_72_METRIC_DATA_REPORT_NEW.json') as f:
    for line in f:
        data.append(json.loads(line))


# In[3]:


part2 = data[60000 : ]


# In[3]:


len(data)


# In[4]:


flattened_list = [flatten(d) for d in part2]


# In[5]:


#portrait_list = list(map(lambda x: {k:v for k, v in x.items() if "portrait" in k}, flattened_list))


# In[6]:


df = pd.DataFrame(flattened_list)


# In[7]:


df.to_csv('part3_300k.csv', index = False)


# In[8]:


df.head()


# In[ ]:




