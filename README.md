# zfslib
ZFS Libraries for Python

This code is based on code from [fs-tools by Rudd-O](https://github.com/Rudd-O/zfs-tools) and is used as a sample of how to use his ZFS libraries to write your own utilities that work with ZFS DataSets / Snapshots.


Notable additions so far:
### get_snapshots()
```
# Query all snapshots in Dataset allowing filter by: 
#  * name: Snapshot name (wildcard sup.), 
#  * dt_from: datetime to start
#  * tdelta: timedelta -or- str(nC) where: n is an integer > 0 and C is one of:
#  ** y=year, m=month, d=day, H=hour, M=minute, s=second. Eg 5H = 5 Hours
#  * dt_to: datetime to stop 
#  * Date search is any combination of (dt_from, dt_to) -or- (dt_from, tdelta) -or- (tdelta, dt_to)
def get_snapshots(ds:Dataset, name:str, dt_from:datetime=None, tdelta=None, dt_to:datetime=None) -> list:
    ...
```
### get_diffs()
```
# Gets Diffs in snapshot or between snapshots (if snap_to is specified)
# ignore - list of glob expressions to ignore (eg ['*_pycache_*'])
# file_type - Filter on the following
    # B       Block device
    # C       Character device
    # /       Directory
    # >       Door
    # |       Named pipe
    # @       Symbolic link
    # P       Event port
    # =       Socket
    # F       Regular file
# chg_type - Filter on the following:
    # -       The path has been removed
    # +       The path has been created
    # M       The path has been modified
    # R       The path has been renamed
def get_diffs(conn:ZFSConnection, snap_from:Snapshot, snap_to:Snapshot=None, ignore:list=None, file_type:str=None, chg_type:str=None) -> list:

```

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

# Load snapshots by name and date/time range
snapshots = zfslib.getSnapshots(ds, name='autosnap*', dt_to=datetime.now(), tdelta=timedelta(hours=24))

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


```
