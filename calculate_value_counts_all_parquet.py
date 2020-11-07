#import pandas as pd
import time
import dask.dataframe as dd
from dask.distributed import Client
from config import *


# 2020-10-21
#client = Client(n_workers=4, threads_per_worker=1, processes=True, memory_limit='7GB')
#In [10]: %run -i calculate_mean_price.py
#/home/ian/data/land_registry/pp-complete_100_nocompression.parquet
#Took 0.0463414192199707s
#New counts N    22795838
#Property type counts:
#...O     299533
#Name: pt, dtype: int64
#Took 3.6024281978607178s
#----
#/home/ian/data/land_registry/pp-complete_100_snappy.parquet
#Took 4.465535402297974s
#----
#/home/ian/data/land_registry/pp-complete_100_gzip.parquet
#Took 4.232710838317871s

#client = Client(n_workers=8, threads_per_worker=1, processes=True, memory_limit='3GB')
#/home/ian/data/land_registry/pp-complete_100_nocompression.parquet
#Took 3.140901565551758s
#/home/ian/data/land_registry/pp-complete_100_snappy.parquet
#Took 3.1817216873168945s
#/home/ian/data/land_registry/pp-complete_100_gzip.parquet
#Took 3.4918391704559326s


# no compression fastest, snappy mid, gzip slightly slower

# 2020-10-19
#In [8]: %run -i calculate_mean_price.py
#Took 0.03252911567687988s for 100 partitions
#New counts N    22795838
#Y     2671675
#Name: new, dtype: int64
#Took 1.6826720237731934s
#----
#Took 0.03278207778930664s for 100 partitions
#New counts N    22795838
#Y     2671675
#Name: new, dtype: int64
#Took 1.9317712783813477s
#----
#Took 0.032685041427612305s for 100 partitions
#New counts N    22795838
#Y     2671675
#Name: new, dtype: int64
#Took 1.9955098628997803s
#----


if __name__ == "__main__":
    if 'client' not in dir():
        #client = Client(n_workers=4, threads_per_worker=1, processes=True, memory_limit='7GB')
        client = Client(n_workers=8, threads_per_worker=1, processes=True, memory_limit='3GB')
        print(client)

    folders = [DATA_PARQUET_100_NOCOMPRESSION, DATA_PARQUET_100_SNAPPY, DATA_PARQUET_100_GZIP]
    for folder in folders:
        print(folder)
        ddf = dd.read_parquet(path=folder)
        t1 = time.time()
        #price = ddf.price.mean().compute()
        #print(f"Mean price {price}")
        new_counts = ddf.new.value_counts().compute()
        print(f"New counts {new_counts}")
        property_type_counts = ddf.pt.value_counts().compute()
        print("Property type counts:")
        print(property_type_counts)
        #print(len(ddf))
        print(f"Took {time.time()-t1}s")
        print("----")

