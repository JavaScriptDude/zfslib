# zfslib

ZFS Libraries for Python

Python library for reading from ZFS Pools. Capable of reading, Pools, Datasets, Snapshots and Diffs. This library is still quite new but will eventually be published as a python package once some implementations are completed on my own end. 

## Installation

Install this plugin using `pip`:

    $ pip install zfslib

## Usage

See examples



Notable additions so far:
### `<Dataset>.get_snapshots(dict)`
```
    # find_snapshots(dict) - Query all snapshots in Dataset and optionally filter by: 
    #  - name: Snapshot name (wildcard supported) 
    #  - dt_from: datetime to start
    #  - tdelta: timedelta -or- string of nC where: n is an integer > 0 and C is one of y,m,d,H,M,S. Eg 5H = 5 Hours
    #  - dt_to: datetime to stop 
    #  - Date searching is any combination of:
    #      (dt_from --> dt_to) | (dt_from --> dt_from + tdelta) | (dt_to - tdelta --> dt_to)
```
### `<Dataset>.get_diffs()`
```
    # get_diffs() - Gets Diffs in snapshot or between snapshots (if snap_to is specified)
    # snap_from - Left side of diff
    # snap_to - Right side of diff. If not specified, diff is to current working version
    # include - list of glob expressions to include (eg ['*_pycache_*'])
    # ignore - list of glob expressions to ignore (eg ['*_pycache_*'])
    # file_type - Filter on the following
    #  - B       Block device
    #  - C       Character device
    #  - /       Directory
    #  - >       Door
    #  - |       Named pipe
    #  - @       Symbolic link
    #  - P       Event port
    #  - =       Socket
    #  - F       Regular file
    # chg_type - Filter on the following:
    #  - -       The path has been removed
    #  - +       The path has been created
    #  - M       The path has been modified
    #  - R       The path has been renamed
```

### `<Snapshot>.snap_path`
```
    # Returns the path to read only zfs_snapshot directory (<ds_mount>/.zfs/snapshots/<snapshot>)
```

### `<Snapshot>.resolve_snap_path(path)`
```
    # Resolves the path to file/dir within the zfs_snapshot dir
    # Returns: tuple(of bool, str) where:
    # - bool = True if item is found
    # - str = Path to item if found else path to zfs_snapshot dir
```

### `<Diff>.snap_path_left`
```
    # Path to resource on left side of diff in zfs_snapshot dir
```

### `<Diff>.snap_path_right`
```
    # Path to resource on right side of diff in .zfs_snapshot dir or working copy
```

See `test.py` for sample code


```python
import zfslib

pool_name = 'rpool'
ds_name = 'devel'

# Read ZFS information from local computer
# Change properties as needed
conn = zfslib.ZFSConnection(host='localhost',properties=["avail", "usedsnap", "usedds", "usedrefreserv", "usedchild", "creation"])


# Load pool
poolset = conn.get_poolset()
pool = poolset.lookup(pool_name)


# Load dataset
ds = pool.get_dataset(ds_name)


# Load snapshots by with name of autosnap* in the last 14 days
snapshots = ds.find_snapshots({'name': 'autosnap*', 'dt_to': datetime.now(), 'tdelta': '14d'})


# Loop through snapshots and analyze diffs
for i, snap in enumerate(snapshots):
    if i > 0:
        # Get Diffs for all files modified with the extension of .py or .js but excluding __pycache__
        diffs = ds.get_diffs(snap_last, snap, file_type='F', chg_type='M', include=['*.py', '*.js'], ignore=['*_pycache_*'])
        for diff in diffs:
            if file_is_text(diff.path_full): # Get diff of any text files
                print('{} - {}'.format(snap.name, diff))
                p_left = diff.snap_path_left
                p_right = diff.snap_path_right
                # Do whatever you want here.
                
    snap_last = snap


```


Credits: This code is based heavily on [zfs-tools by Rudd-O](https://github.com/Rudd-O/zfs-tools).