#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import time
import copy
import category_encoders as ce
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import warnings
warnings.filterwarnings('ignore')
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
import fastcluster as fs
import seaborn as sns


# In[2]:


df = pd.read_csv('fp_dataset.csv', low_memory = False)


# In[3]:


def missing_data_table(df):
    total = df.isnull().sum().sort_values(ascending=True)
    percent = (df.isnull().sum()/df.isnull().count()).sort_values(ascending=True)
    missing_data = pd.concat([total, percent], axis=1, keys=['Total', 'Percent'])
    return missing_data

missing_data_table(df)


# In[4]:


cols = []
def candid(dataframe):
    i = 1
    for c in dataframe.columns.tolist():
        if (dataframe[c].isnull().sum() <= 20000) == True:
            i += 1
            cols.append(c)
    return cols

candid(df)


# In[5]:


df2 = df[cols]


# In[6]:


print(df2.shape)
df2.head()


# In[7]:


#I am renaming column names because they are too redundant
column_names = []
cols = df2.columns.tolist()
for col in cols:
    pattern = "METRIC_DATA_metricBreakdowns_fingerprints_"
    if pattern in col:
        column_names.append(col[42:])
    else:
        column_names.append(col)
        
df2.set_axis(column_names, axis = 1, inplace = True)

df2.drop(['ID', '_id_$oid', 'METRIC_DATA_uniqueId'], axis = 1, inplace = True)


# In[8]:


df2.head()


# In[9]:


print("Total number of null values in the df2 dataframe: ", format(df2.isnull().values.sum()))
df2.isnull().sum()


# In[10]:


print(df2['fingerprint_0_type'].unique())
print(df2['fingerprint_1_type'].unique())
print(df2['fingerprint_2_type'].unique())
print(df2['fingerprint_3_type'].unique())


# In[11]:


fp_dict = {'RIGHT_THUMB': 1, 'RIGHT_INDEX_FINGER': 2, 'LEFT_INDEX_FINGER': 3, 'LEFT_THUMB': 4}


# In[12]:


df2a = df2.copy()


# In[13]:


df2a['fingerprint_0_type'] = df2a['fingerprint_0_type'].replace(fp_dict)
df2a['fingerprint_1_type'] = df2a['fingerprint_1_type'].replace(fp_dict)
df2a['fingerprint_2_type'] = df2a['fingerprint_2_type'].replace(fp_dict)
df2a['fingerprint_3_type'] = df2a['fingerprint_3_type'].replace(fp_dict)


# In[14]:


df2a.head()


# In[15]:


encoder = ce.BinaryEncoder(cols=['METRIC_DATA_location'])
df2a = encoder.fit_transform(df2a)


# In[16]:


df2a.head()


# In[17]:


encoder1 = ce.BinaryEncoder(cols=['METRIC_DATA_kitTag'])
df2a = encoder1.fit_transform(df2a)


# In[18]:


df2a.shape


# In[19]:


df2a.head()


# In[20]:


pd.set_option('display.max_columns', 150)
df2a.loc[df2a['fingerprint_0_type'].isnull()]


# In[21]:


df2a.loc[df2a['fingerprint_0_type'].isnull()].count()


# In[22]:


df2a = df2a[np.isfinite(df2a['fingerprint_0_type'])]


# In[23]:


df2a.shape


# In[24]:


df2a.head()


# In[25]:


df2a.fingerprint_0_attempts_0_attemptBreakdowns_1_property.unique()


# In[26]:


property_dict = {'MinNumberBlocks': 1, 'MaxNumberPoorRidgeFlow': 2,
       'MaxNumberLightBlocks' : 3, 'MinNumberGoodBlocks': 4, 'MinNFIQ' : 5,
       'MaxNumberDarkBlocks': 6, 'MaxNotPartOfPrint': 7, 'MinAFIQ': 8 }


# In[27]:


property_cols= []
cols = df2a.columns.tolist()
for col in cols:
    pattern = 'property'
    if pattern in col:
        property_cols.append(col)
    else:
        pass
    
property_cols  


# In[28]:


def replace_property():
    for col in property_cols:
         df2a[col] = df2a[col].replace(property_dict)
        
replace_property()


# In[29]:


df2a.head()


# In[30]:


df2a.fingerprint_0_attempts_0_attemptBreakdowns_1_success.unique()


# In[31]:


success_cols= []
cols = df2a.columns.tolist()
for col in cols:
    pattern = 'success'
    if pattern in col:
        success_cols.append(col)
    else:
        pass
success_cols


# In[32]:


for col in success_cols:
         df2a[col] = df2a[col].replace(property_dict)


# In[33]:


def fill_empty(dataframe, columns):
    for col in columns:
        dataframe[col].fillna(value = False, inplace = True)
    return dataframe[columns].isnull().sum()

fill_empty(df2a, success_cols)


# In[34]:


def fill_empty(dataframe, columns):
    for col in columns:
        dataframe[col].fillna(value = 9999, inplace = True)
    return dataframe[columns].isnull().sum()

fill_empty(df2a, property_cols)


# In[35]:


value_cols= []
cols = df2a.columns.tolist()
for col in cols:
    pattern = 'value'
    if pattern in col:
        value_cols.append(col)
    else:
        pass


# In[36]:


def fill_empty(dataframe, columns):
    for col in columns:
        dataframe[col].fillna(value = 0.12, inplace = True)
    return dataframe[columns].isnull().sum()

fill_empty(df2a, value_cols)


# In[37]:


df2a.isnull().sum().sort_values()


# In[38]:


df2a.fingerprint_0_attempts_0_deviceType.value_counts()


# In[39]:


df2a.fingerprint_1_attempts_0_deviceType.value_counts()


# In[40]:


df2a.fingerprint_2_attempts_0_deviceType.value_counts()


# In[41]:


df2a.fingerprint_3_attempts_0_deviceType.value_counts()


# In[42]:


device_cols= []
cols = df2a.columns.tolist()
for col in cols:
    pattern = 'device'
    if pattern in col:
        device_cols.append(col)
    else:
        pass
device_cols


# def fill_device_cols(df):
#     for col in device_cols:
#         mode = df[col].mode()
#         df[col] = df[col].fillna(mode, inplace = True)
#     
# fill_device_cols(df2a)

# In[43]:


df2a.isnull().values.sum()


# In[45]:


def fill_device_cols(dataframe, columns):
    for col in columns:
        dataframe[col].fillna(value = 'Not Captured', inplace = True)
    return dataframe[columns].isnull().sum()

fill_empty(df2a, device_cols)


# In[47]:


device_options = df2a.fingerprint_2_attempts_0_deviceType.unique().tolist()


# In[48]:


device_options


# In[49]:


device_dict = {0.12: 0,
 'FS80': 1,
 'FS10': 2,
 'FS88H': 3,
 'FS80H': 4,
 'DigitalPersona, Inc.': 5,
 'UNIDENTIFIED': 6,
 'FS90': 7,
 'FS88': 8,
 'Futronic': 9}


# In[ ]:




