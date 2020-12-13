# zfslib
ZFS Libraries for Python

This code is based on code from [fs-tools by Rudd-O](https://github.com/Rudd-O/zfs-tools) and is used as a sample of how to use his ZFS libraries to write your own utilities that work with ZFS DataSets / Snapshots.

See `test.py` for sample code

```python
from connection import ZFSConnection

pool_name = 'rpool'
ds_name = 'devel'

# Read ZFS information from local computer
# Change properties as needed
src_conn = ZFSConnection(host='localhost',properties=["name", "avail", "usedsnap", "usedds", "usedrefreserv", "usedchild", "creation"])

# Load pool
pool = src_conn.pools.lookup(pool_name)

# Load dataset
ds = pool.get_child(ds_name)

ds_name_full = f"{pool_name}/{ds_name}"

# Print datasets creation date
print(f"{ds_name_full} creation date: {ds.get_creation()}")

# Grab Snapshots
snapshots = ds.get_snapshots()

# Load snapshot by index or iterate and filter as required
snap = snapshots[1]

# Get Snapshot Creation date
print(f"{ds_name_full}@{snap.name} creation date: {ds.get_creation()}")

# Read property from DataSet / Snapshot
print(f"{ds_name_full}@{snap.name} usedsnap: {snap.get_property('usedsnap')}")


```
