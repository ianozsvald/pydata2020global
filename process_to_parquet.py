#import pandas as pd
import time
import dask.dataframe as dd
from dask.distributed import Client
from config import *

# 2020-10-21
#Took 52.72236776351929s for 100 partitions
#Took 51.56036853790283s for 100 snappy partitions
#Took 103.02895617485046s for 100 gzip partitions
#(base) ian@ian-XPS-15-9550:land_registry$ du -h .
#1.5G	./pp-complete_100_snappy.parquet
#2.2G	./pp-complete_100_nocompression.parquet
#979M	./pp-complete_100_gzip.parquet


# 2020-10-19
# each for 100 partitions (note default compression but prior to snappy being installed, so no compression)
# client = Client(n_workers=1, threads_per_worker=1, processes=True, memory_limit='16GB'), no warnings, 151s
#client = Client(n_workers=2, threads_per_worker=1, processes=True, memory_limit='16GB'), no warnings, 78s
#client = Client(n_workers=4, threads_per_worker=1, processes=True, memory_limit='7GB'), no warnings, 50s
# client = Client(n_workers=1, threads_per_worker=4, processes=True, memory_limit='7GB'), no warnings, 118s
#client = Client(n_workers=2, threads_per_worker=4, processes=False, memory_limit='2GB'), memory warnings, maybe didn't complete

#client = Client(n_workers=4, threads_per_worker=1, processes=True, memory_limit='7GB') 50s for 100 or 200 partitions (same)

#client = Client(n_workers=4, threads_per_worker=1, processes=True, memory_limit='7GB') 50s 

# no compression, snappy, gzip
#client = Client(n_workers=4, threads_per_worker=1, processes=True, memory_limit='7GB') 44s no compression, 50s snappy, 95s gzip
#2.2G	./pp-complete_100_nocompression.parquet
#1.5G	./pp-complete_100_snappy.parquet
#979M	./pp-complete_100_gzip.parquet



if __name__ == "__main__":
    if 'client' not in dir():
        client = Client(n_workers=4, threads_per_worker=1, processes=True, memory_limit='7GB')
        print(client)

    columns = ['tin', 'price', 'date', 'postcode', 'pt', 'new', 'duration', 'paon', 'saon',
               'street', 'locality', 'town', 'district', 'county', 'ppd_cat', 'status']
    # 4.4GB input
    #RAW_DATA = "~/data/land_registry/pp-complete-202009.csv"
    #OUT_FOLDER = "~/data/land_registry/processed_pydataglobal2020/"
    print("Reading csv")
    ddf = dd.read_csv(RAW_DATA, names=columns, parse_dates=['date'])
    print(ddf.info)

    ddf_partitioned = ddf.repartition(npartitions=100)
    print(ddf_partitioned.npartitions)
    print(ddf_partitioned.divisions)

    t1 = time.time()
    ddf_partitioned.to_parquet(path=DATA_PARQUET_100_NOCOMPRESSION, compression=None)
    print(f"Took {time.time()-t1}s for 100 partitions")

    t1 = time.time()
    ddf_partitioned.to_parquet(path=DATA_PARQUET_100_SNAPPY, compression="snappy")
    print(f"Took {time.time()-t1}s for 100 snappy partitions")
    
    t1 = time.time()
    ddf_partitioned.to_parquet(path=DATA_PARQUET_100_GZIP, compression="gzip")
    print(f"Took {time.time()-t1}s for 100 gzip partitions")

    if False:
        # note we get 74 partitions, not 10!
        ddf_partitioned = ddf.repartition(npartitions=10)
        print(ddf_partitioned.npartitions)
        print(ddf_partitioned.divisions)
        t1 = time.time()
        ddf.to_parquet(path=DATA_PARQUET_10_SNAPPY, compression="snappy")
        print(f"Took {time.time()-t1}s for 10 snappy partitions")

        t1 = time.time()
        ddf.to_csv(filename=DATA_CSV)
        print(f"Took {time.time()-t1}s for 10 csv partitions")

