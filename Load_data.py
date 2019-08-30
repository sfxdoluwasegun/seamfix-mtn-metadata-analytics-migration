# Necessary libraries to be installed are: dask, flatten_json, json(comes pre-installed)

import json
import dask.dataframe as dd
import dask.delayed as delayed
import flatten_json
from flatten_json import flatten
print('All libraries in')
data = []
with open('METRIC_DATA_REPORT_NEW_view.json') as f:
    for line in f:
        data.append(json.loads(line))
print('... JSON object already read into the data object')
dic_flattened = [flatten(d) for d in data]
print('... data has been flattened successfully')
z = delayed(dic_flattened)
print('Flattened data has been moved to a delayed object')
df = dd.from_delayed(z)
df.to_csv('metadata_file-*.csv', index = False)

