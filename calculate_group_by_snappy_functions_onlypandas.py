import pandas as pd
import time
import dask.dataframe as dd
from dask.distributed import Client
from config import *


# 2020-10-21
#$ mprof run  calculate_group_by_snappy_functions_onlypandas.py

@profile
def vc(df):
    t1 = time.time()
    property_type_counts = df.pt.value_counts()
    print("Property type counts:")
    print(property_type_counts)
    print(f"Took {time.time()-t1}s")

@profile
def gpby(df):
    t1 = time.time()
    property_type_counts = df.groupby('pt').size()
    print("Property type counts:")
    print(property_type_counts)
    print(f"Took {time.time()-t1}s")
    print("----")

if __name__ == "__main__":

    t1 = time.time()
    df = pd.read_pickle(PICKLE)
    print(f"read_pickle took {time.time()-t1:0.2f}s")
    print(f"shape {df.shape}")

    vc(df)
    #time.sleep(2)
    #gpby(df)
    

