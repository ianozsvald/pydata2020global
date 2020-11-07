# let's try an operation
import vaex
from config import *

#@profile
def open():
    df = vaex.open(VAEX)
    print(f"df has shape {df.shape}")
    return df

#@profile
def groupby_pt_count(vdf):
    res = vdf.groupby('pt', 'count')
    print(res)
    return res

#@profile
def groupby_year_pt_count(vdf):
    res = vdf.groupby(['year', 'pt'], 'count')
    print(res)
    return res

#df = open()
#res = valuecounts(df)

vdf = open()

print(f'Mean for price column: {vdf.price.mean():,0.2f}')

vdf['year'] = vdf.date.dt.year

res = groupby_year_pt_count(vdf)

res = groupby_pt_count(vdf)

