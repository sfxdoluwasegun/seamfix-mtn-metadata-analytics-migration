# Necessary libraries to be installed are: dask, flatten_json, json(comes pre-installed)

if __name__ == '__main__':
    import json
    import pandas as pd
    import flatten_json
    from flatten_json import flatten
    print('All libraries in')
    data = []
    with open('METRIC_DATA_REPORT_NEW_view.json') as f:
        for line in f:
            data.append(json.loads(line))
    print('... JSON object already read into the data object')
    flattened_list = [flatten(d) for d in data]
    print('... Data has been flattened successfully')
    first_set = flattened_list[0:20000]
    df = pd.DataFrame(first_set)
    df.to_csv('part1.csv', index=False)
    print('... first batch of meta data records have been saved to the local drive')
    second_set = flattened_list[20000:]
    df = pd.DataFrame(second_set)
    df.to_csv('part2.csv', index = False)
    print('... second batch of meta data records have been saved to the local drive')
    print('--------- End of code ---------')
