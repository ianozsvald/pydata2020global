# load raw CSV into Pandas
# need to check groupby expense
# 

# 2020-10-20
#$ python load_csv_to_pandas.py  # tops at 14GB, drops back to 11GB after read_csv
#read_csv took 132.15s
#shape (25467513, 16)
#memoryusage: 22628831189#  reports 22GB but RES and VIRT clearly are less!
#dfs.to_sql("landreg", con=engine)
# climbs to 20GB+ whilst writing to SQL and machine got to 0GB spare, it carried on at that point
# note did have echo on

# dfs.to_sql("landreg", con=engine, chunksize=100_000)
# climbs to 17GB with 12GB RAM free
# Took 716.69s to write to sqlite, 4.6GB output sqlite3 file

# possible insert speedup
# https://www.codementor.io/@bruce3557/graceful-data-ingestion-with-sqlalchemy-and-pandas-pft7ddcy6

# 3633548220 Oct 21 18:20 pp-complete-202009.pickle


import os
import time
import sqlalchemy 
import pandas as pd
from config import *

#RAW_DATA = "~/data/land_registry/pp-complete-202009.csv"
#PICKLE = "~/data/land_registry/pp-complete-202009.pickle"
# .gz" cant as takes too long!

columns = ['tin', 'price', 'date', 'postcode', 'pt', 'new', 'duration', 'paon', 'saon',
           'street', 'locality', 'town', 'district', 'county', 'ppd_cat', 'status']
# 4.4GB input
t1 = time.time()
df = pd.read_csv(RAW_DATA, names=columns, parse_dates=['date'])
print(f"read_csv took {time.time()-t1:0.2f}s")
print(f"shape {df.shape}")

df.to_pickle(PICKLE)
