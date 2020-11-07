#import pandas as pd
import time
import dask.dataframe as dd
from dask.distributed import Client
from config import *


# 2020-10-21

def vc(ddf):
    t1 = time.time()
    property_type_counts = ddf.pt.value_counts().compute()
    print("Property type counts:")
    print(property_type_counts)
    print(f"Took {time.time()-t1}s")

def gpby(ddf):
    t1 = time.time()
    property_type_counts = ddf.groupby('pt').size().compute()
    print("Property type counts:")
    print(property_type_counts)
    print(f"Took {time.time()-t1}s")
    print("----")

if __name__ == "__main__":
    if 'client' not in dir():
        client = Client(n_workers=4, threads_per_worker=1, processes=True, memory_limit='7GB')
        #client = Client(n_workers=8, threads_per_worker=1, processes=True, memory_limit='7GB')
        print(client)

    folder = DATA_PARQUET_100_SNAPPY
    print(folder)
    ddf = dd.read_parquet(path=folder)

    vc(ddf)
    #time.sleep(2)
    #gpby(ddf)
    

