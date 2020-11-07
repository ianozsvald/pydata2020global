import time
import dask.dataframe as dd
from dask.distributed import Client
from config import *

#client = Client(n_workers=10, threads_per_worker=1, processes=True, memory_limit='3GB')
#DATA = DATA_PARQUET_10_SNAPPY 
# 2.6-3s

#client = Client(n_workers=10, threads_per_worker=1, processes=True, memory_limit='3GB')
# DATA = DATA_PARQUET_100_SNAPPY
# 3.6-4.9s

# client = Client(n_workers=2, threads_per_worker=1, processes=True, memory_limit='7GB')
# DATA = DATA_PARQUET_100_SNAPPY
# 7-8s

#client = Client(n_workers=2, threads_per_worker=1, processes=True, memory_limit='7GB')
#DATA = DATA_PARQUET_10_SNAPPY 
# 5-5.5s

# note 32 workers is slower
#client = Client(n_workers=16, threads_per_worker=1, processes=True, memory_limit='3GB')
# DATA = DATA_PARQUET_100_SNAPPY
# 3.8s

if __name__ == "__main__":
    if 'client' not in dir():
        #client = Client(n_workers=32, threads_per_worker=1, processes=True, memory_limit='3GB')
        client = Client(n_workers=16, threads_per_worker=1, processes=True, memory_limit='3GB')
        #client = Client(n_workers=10, threads_per_worker=1, processes=True, memory_limit='3GB')
        #client = Client(n_workers=2, threads_per_worker=1, processes=True, memory_limit='7GB')
    print(client)

    columns = ['county', 'pt'] # more efficient process
    #DATA = DATA_PARQUET_10_SNAPPY 
    DATA = DATA_PARQUET_100_SNAPPY
    #DATA = DATA_PARQUET_10_NOCOMPRESSION
    ddf = dd.read_parquet(DATA, 
                          columns=columns)

    #%%time
    t1 = time.time()
    result = ddf.groupby(['county', 'pt']).count()
    result = result.compute()
    print(f'Took {time.time()-t1:0.2f}s')
    print(result)