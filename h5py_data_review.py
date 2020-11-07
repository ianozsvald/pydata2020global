import h5py

f = h5py.File('/home/ian/data/land_registry/pp-complete-202009.csv.hdf5', 'r')
tbl = f['table']
cols = tbl['columns']
# driver is sec2 - Unbuffered, optimized I/O using standard POSIX functions.
print('Keys in table.columns: ', cols.keys())
print(f'Length of table: {len(tbl)}')

print()
print('Columns and details, note shape for S1 strings seems to be length of all binary data, not row count')
for col_name in cols.keys():
    col = cols[col_name]['data']
    print(col.name, col.dtype, col.shape)

print()
print('First few items from price column:')
price = cols['price']['data']
print('Prices: ', price[:10])
