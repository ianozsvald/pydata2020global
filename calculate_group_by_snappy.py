#import pandas as pd
import time
import dask.dataframe as dd
from dask.distributed import Client
from config import *


# 2020-10-21

#client = Client(n_workers=4, threads_per_worker=1, processes=True, memory_limit='7GB')
# vc vs gpby on new
#Took 1.5142455101013184s
#Took 14.248690366744995s
# vc vs gpby on pt
#Took 1.5026183128356934s
#Took 13.982444286346436s
#client = Client(n_workers=8, threads_per_worker=1, processes=True, memory_limit='7GB')
# doubling n_workers gives same speed

# learning to plot
# mprof run --multiprocess python calculate_group_by_snappy.py
#  mprof run --include-children --multiprocess python calculate_group_by_snappy.py
#  mprof run --include-children  python calculate_group_by_snappy.py


if __name__ == "__main__":
    if 'client' not in dir():
        client = Client(n_workers=4, threads_per_worker=1, processes=True, memory_limit='7GB')
        #client = Client(n_workers=8, threads_per_worker=1, processes=True, memory_limit='7GB')
        print(client)

    folder = DATA_PARQUET_100_SNAPPY
    print(folder)
    ddf = dd.read_parquet(path=folder)
    t1 = time.time()
    new_counts = ddf.new.value_counts().compute()
    print(f"New vc counts {new_counts}")
    print(f"Took {time.time()-t1}s")

    t1 = time.time()
    new_counts = ddf.groupby('new').size().compute()
    print(f"New gpby counts {new_counts}")
    print(f"Took {time.time()-t1}s")

    t1 = time.time()
    property_type_counts = ddf.pt.value_counts().compute()
    print("Property type counts:")
    print(property_type_counts)
    print(f"Took {time.time()-t1}s")

    t1 = time.time()
    property_type_counts = ddf.groupby('pt').size().compute()
    print("Property type counts:")
    print(property_type_counts)
    print(f"Took {time.time()-t1}s")
    print("----")

