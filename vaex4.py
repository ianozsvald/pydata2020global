# let's try an operation
import time
import vaex
from config import *

#@profile
def open():
    print(f"df has shape {df.shape}")
    return df

#@profile
def groupby(df):
    res = df.groupby('pt', 'count')
    print(res)
    return res

#@profile
def valuecounts(df):
    res = df.pt.value_counts()
    print(res)
    return res

#df = open()
#res = valuecounts(df)

#df = open()
#res = groupby(df)


vdf = vaex.open(VAEX) # filename
#vdf['dayofweek'] = vdf.date.dt.dayofweek
t1 = time.time()
vdf['year'] = vdf.date.dt.year
counts = vdf.groupby(['year', 'pt', 'new'], 'count')
#counts = counts.sort(by='dayofweek')
print(f'Took {time.time()-t1:0.2f}s')
print(counts)
