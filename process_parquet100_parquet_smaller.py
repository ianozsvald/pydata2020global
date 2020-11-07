#import pandas as pd
import time
import dask.dataframe as dd
from dask.distributed import Client
from config import *




if __name__ == "__main__":
    if 'client' not in dir():
        client = Client(n_workers=4, threads_per_worker=1, processes=True, memory_limit='7GB')
        print(client)

    ddf = dd.read_parquet(DATA_PARQUET_100_SNAPPY)
    print(ddf.info())
    print(ddf.npartitions)
    dfx = ddf.get_partition(0).compute()
    print(dfx.info(memory_usage='deep'))
    del dfx

    ddf_partitioned = ddf.repartition(npartitions=10)
    print(ddf_partitioned.npartitions)
    print(ddf_partitioned.divisions)
    
    ddf.repartition(npartitions=10)
    t1 = time.time()
    #ddf_partitioned.to_parquet(path=DATA_PARQUET_10_SNAPPY, compression='snappy')
    ddf_partitioned.to_parquet(path=DATA_PARQUET_10_NOCOMPRESSION, compression=None)
    print(f"Took {time.time()-t1}s for 10 partitions")
    dfx = ddf_partitioned.get_partition(0).compute()
    print(dfx.info(memory_usage='deep'))
    del dfx

