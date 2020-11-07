import os

ROOT = os.path.expanduser('~/data/land_registry')
RAW_DATA = os.path.join(ROOT, "pp-complete-202009.csv")
PICKLE = os.path.join(ROOT, "pp-complete-202009.pickle")
SQLITEDB = os.path.join(ROOT, "pp-complete-202009.sqlite")
VAEX = os.path.join(ROOT, "pp-complete-202009.csv.hdf5")

DATA_PARQUET_100_NOCOMPRESSION = os.path.join(ROOT, 'pp-complete_100_nocompression.parquet')
DATA_PARQUET_100_SNAPPY = os.path.join(ROOT, 'pp-complete_100_snappy.parquet')
DATA_PARQUET_100_GZIP = os.path.join(ROOT, 'pp-complete_100_gzip.parquet')
DATA_PARQUET_10_SNAPPY = os.path.join(ROOT, 'pp-complete_10_snappy.parquet')
DATA_PARQUET_10_NOCOMPRESSION = os.path.join(ROOT, 'pp-complete_10_nocompression.parquet')

DATA_PARQUET_10_SNAPPY = os.path.join(ROOT, 'pp-complete_10_snappy.parquet')

DATA_CSV = os.path.join(ROOT, 'pp-complete_10.csv')

COLUMNS = ['tin', 'price', 'date', 'postcode', 'pt', 'new', 'duration', 'paon', 'saon',
           'street', 'locality', 'town', 'district', 'county', 'ppd_cat', 'status']

