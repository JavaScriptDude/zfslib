# See: https://github.com/JavaScriptDude/zfslib

import zfslib
import fnmatch
from datetime import datetime, timedelta

pool_name = 'rpool'
ds_name = 'devel'



# Read ZFS information from local computer
# Change properties as needed
src_conn = zfslib.ZFSConnection(host='localhost', properties=["name", "avail", "usedsnap", "usedds", "usedrefreserv", "usedchild", "creation"])

# Load pool
pool = src_conn.pools.lookup(pool_name)

# Load dataset
ds = pool.get_child(ds_name)



# Load snapshots by name and date/time range
snapshots = zfslib.get_snapshots(ds, name='autosnap*', dt_to=datetime.now(), tdelta=timedelta(hours=24))

for snap in snapshots:
    diffs = zfslib.get_diffs(src_conn, snap_from=snap, ignore=['*.vscod*', '*_pycache_*'])
    for diff in diffs:
        print('{} - {}'.format(snap.name, diff))


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
