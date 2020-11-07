import time
import dask.dataframe as dd
from dask.distributed import Client
from config import *

# TODO giles suggests doing categorical encoding - does hdf5 do this internally?

#DATA = DATA_PARQUET_10_SNAPPY  # note 250MB partitions
#columns = None
#client = Client(n_workers=32, threads_per_worker=1, processes=True, memory_limit='3GB')
# it succeeds in 25s 

#DATA = DATA_PARQUET_10_SNAPPY  # note 250MB partitions
#columns = ['date', 'pt', 'new'] # more efficient process
#client = Client(n_workers=32, threads_per_worker=1, processes=True, memory_limit='3GB')
# succeeds in 3s

#DATA = DATA_PARQUET_10_SNAPPY  # note 250MB partitions
#columns = ['date', 'pt', 'new'] # more efficient process
#client = Client(n_workers=10, threads_per_worker=1, processes=True, memory_limit='3GB')
# succeeds in 3s

if __name__ == "__main__":
    if 'client' not in dir():
        #client = Client(n_workers=32, threads_per_worker=1, processes=True, memory_limit='3GB')
        client = Client(n_workers=10, threads_per_worker=1, processes=True, memory_limit='3GB')
        #client = Client(n_workers=2, threads_per_worker=1, processes=True, memory_limit='7GB')
        print(client)

    columns = ['date', 'pt', 'new'] # more efficient process
    #columns = None # used in conf talk
    DATA = DATA_PARQUET_10_SNAPPY 
    #DATA = DATA_PARQUET_10_NOCOMPRESSION
    #DATA = DATA_PARQUET_100_SNAPPY
    ddf = dd.read_parquet(DATA, 
                          columns=columns)

    #ddf.head()

    #%%time
    t1 = time.time()
    ddf['year'] = ddf.date.dt.year
    result = ddf.groupby(['year', 'pt', 'new']).count()
    result.visualize()
    result = result.compute()
    print(f'Took {time.time()-t1:0.2f}s')
    print(result)

    t1 = time.time()
    _ = ddf.get_partition(0).compute()
    print(f'Took {time.time()-t1:0.2f}s to just load partition 0')
