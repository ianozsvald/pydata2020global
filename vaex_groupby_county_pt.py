# let's try an operation
import time
import vaex
from config import *

# This takes circa 1s

def open():
    df = vaex.open(VAEX)
    print(f"df has shape {df.shape}")
    return df

def groupby(vdf):
    res = vdf.groupby(('county', 'pt'), 'count')
    print(res)
    return res

vdf = open()

t1 = time.time()
res = groupby(vdf)
print(f'Took {time.time()-t1:0.2f}s')
print(res)


