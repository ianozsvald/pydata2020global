import time
import dask.dataframe as dd
from dask.distributed import Client
from config import *

if __name__ == "__main__":
    if 'client' not in dir():
        client = Client(n_workers=8, threads_per_worker=1, processes=True, memory_limit='3GB')
        #client = Client(n_workers=4, threads_per_worker=1, processes=True, memory_limit='7GB')
        print(client)

    columns = ['date', 'pt']
    ddf = dd.read_parquet(DATA_PARQUET_100_SNAPPY, 
                          columns=columns)
    #ddf.head()

    #%%time
    t1 = time.time()
    qry = 'date >= "2018-01-01" and date < "2019-12-29"'
    ddf_s = ddf.query(qry).set_index('date') # previously unindexed
    weekly_completions = ddf_s['pt'].resample('W').count().compute()
    print(f'Took {time.time()-t1:0.2}s')
    print(weekly_completions)
