import time
from config import *
import vaex

# from_csv with convert=True writes the converted file back to the same folder
# takes 346s (circa 5mins) to write to 
# 4835469885 Oct 23 17:46 pp-complete-202009.csv.hdf5 (4.8GB file)
#RAW_DATA = '/home/ian/data/land_registry/pp-complete-202009.short.csv'
print(f"Processing from {RAW_DATA}")
t1 = time.time()
df = vaex.from_csv(RAW_DATA, copy_index=False, chunk_size=None, convert=True, parse_dates=['date'], names=COLUMNS) 
print(f"Took {time.time()-t1:0.1}f")
