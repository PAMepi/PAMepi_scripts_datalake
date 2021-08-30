from datetime import datetime
from pyspark.sql.functions import lit


def union_all(dfs):
    cols = set()

    for df in dfs:
        for col in df.columns:
            cols.add(col)
    cols = sorted(cols)

    new_dfs = {}

    for i, d in enumerate(dfs):
        new_name = 'df' + str(i)
        new_dfs[new_name] = d

        for col in cols:
            if col not in d.columns:
                new_dfs[new_name] = new_dfs[new_name].withColumn(col,
                                                            lit(0))
        new_dfs[new_name] = new_dfs[new_name].select(cols)
    result = new_dfs['df0']
    dfs_to_add = new_dfs.keys()
    keys = list(dfs_to_add)
    keys.remove('df0')

    for x in dfs_to_add:
        result = result.union(new_dfs[x])
    return result
