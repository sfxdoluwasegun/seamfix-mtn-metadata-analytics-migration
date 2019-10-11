#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import copy
pd.set_option('display.max_columns', 40000)
pd.set_option('display.max_columns', 218)


# In[2]:


main = pd.read_csv('portrait_3.csv', low_memory = False)


# In[3]:


att0 = main.columns[main.columns.str.contains(pat = 'METRIC_DATA_metricBreakdowns_portrait_attempts_0_')]
att1 = main.columns[main.columns.str.contains(pat = 'METRIC_DATA_metricBreakdowns_portrait_attempts_1_')]
att2 = main.columns[main.columns.str.contains(pat = 'METRIC_DATA_metricBreakdowns_portrait_attempts_2_')]
att3 = main.columns[main.columns.str.contains(pat = 'METRIC_DATA_metricBreakdowns_portrait_attempts_3_')]
att4 = main.columns[main.columns.str.contains(pat = 'METRIC_DATA_metricBreakdowns_portrait_attempts_4_')]


# In[4]:


df1 = main[att1]
df0 = main[att0]
df2 = main[att2]
df3 = main[att3]
df4 = main[att4]


# In[5]:


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


# In[6]:


df0.set_axis(column_names_0, axis = 1, inplace = True)
df1.set_axis(column_names_1, axis = 1, inplace = True)
df2.set_axis(column_names_2, axis = 1, inplace = True)
df3.set_axis(column_names_3, axis = 1, inplace = True)
df4.set_axis(column_names_4, axis = 1, inplace = True)


# In[7]:


total = [df1, df2, df3, df4, df0]
final = pd.concat(total, axis = 0, sort = True)


# In[8]:


final.shape


# In[9]:


final.status.value_counts()


# In[10]:


final_false = final[final['status']==False]


# In[11]:


final_false.head()


# In[12]:


final_false.fillna(-159.78, inplace = True)


# In[13]:


props = pd.read_csv('hard_factsx5.csv')


# In[14]:


final_false.head()


# In[15]:


props.head()


# In[16]:


properties_list = list(props['property'].values)


# In[17]:


property_cols = []
value_cols = []
pattern_1 = 'property'
pattern_2 = 'value'
for col in column_names_0:
    if pattern_1 in col:
        property_cols.append(col)
    elif pattern_2 in col:
        value_cols.append(col)
    else:
        pass


# In[18]:


for i in property_cols:
    count = 1
    num1 = int(i.split('_')[1])
    for j in value_cols:
        if num1 == int(j.split('_')[1]):
            final_false['pair_'+ str(num1)] = final_false[[i, j]].apply(tuple, axis = 1)
            count += 1


# In[19]:


x = main.columns[main.columns.str.contains('capture')]
x


# In[20]:


property_cols[:3]


# In[21]:


final_false.shape


# In[22]:


final_false.head()


# In[23]:


pairs = final_false.columns[final_false.columns.str.contains(pat = 'pair')]


# In[24]:


pairs_df = final_false[pairs]


# In[25]:


pairs_df.shape


# In[26]:


pairs_df.head()


# In[27]:


pairs_df.dtypes


# In[28]:


h = pairs_df.iloc[0:1,0:1]
h


# In[29]:


import sys
import keyword


# In[30]:


#list(filter(lambda x: y for y in x if x))


# In[31]:


property_dict = {a:{"maxi":0, "minim":sys.maxsize, "count":0, "total":0} for a in properties_list}

for elem in pairs_df.values:
    for a, b in elem:
        if (a == '-159.78') or b == -159.78:
            continue
        try:
            propertyDict = property_dict[a]
        except KeyError:
            continue
        if type(b) == str:
            print(f"Omitting values for property {a} with value {b} ")
#             continue
        if float(b) < propertyDict['minim']:
            propertyDict['minim'] = float(b)
        if float(b) > propertyDict['maxi']: 
            propertyDict['maxi'] = float(b)
        propertyDict['count'] +=1
        propertyDict['total'] += float(b)


# In[32]:


property_dict


# In[33]:


tester = pd.DataFrame.from_dict(property_dict).T


# In[34]:


tester.head()


# In[35]:


tester.dtypes


# In[36]:


tester.shape


# In[37]:


#tester['property'] = tester.index


# In[38]:


#tester.shape[0]


# In[39]:


#type(tester.index)


# In[40]:


#new_index = list(range(0, tester.shape[0]))


# In[41]:


tester.reset_index(inplace = True)


# In[42]:


tester.head()


# In[43]:


#tester.drop('property', axis = 1, inplace = True)


# In[44]:


tester.to_csv('stats_part3.csv', index = False)


# In[ ]:




