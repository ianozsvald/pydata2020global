# let's try an operation
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
vdf['dayofweek'] = vdf.date.dt.dayofweek
counts = vdf.groupby('dayofweek', 'count')
counts = counts.sort(by='dayofweek')

import matplotlib.pyplot as plt
import matplotlib as mpl
fig, ax = plt.subplots(constrained_layout=True)
dayofweek_dict = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
counts_df = counts.to_pandas_df().set_index('dayofweek').rename(index=dayofweek_dict)
counts_df.index.name='Days'
counts_df.plot(ax=ax, kind='bar')
ax.set_title("Completions by day across dataset")
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.grid(axis='y') # horizontal grid lines only
ax.get_yaxis().set_major_formatter(mpl.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
ax.set_ylabel("Completions");
fig.savefig('vaex3_counts.png')
#ax.legend(frameon=False)
