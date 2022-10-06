# zfslib

ZFS Libraries for Python

Python library for reading from ZFS Pools. Capable of reading, Pools, Datasets, Snapshots and Diffs for use by other Python Tools.

This tool presently targets Python v3.7+

## Installation

Install this plugin using `pip`:

    $ python3 -m pip install zfslib

## Usage

See examples folder

## Sample code

```
    import zfslib as zfs

    # Read ZFS information from local computer
    # For remote computer access, use certificate based ssh authentication 
    # see `examples/ex_remote.py`
    conn = zfs.Connection(host='localhost')


    # Load poolset. 
    # zfs properties can be queried here with: zfs_prop=['prop1','prop2',...]
    # zpool properties can be queried here with: zpool_props=['prop1','prop2',...]
    # Default properties: name, creation
    # If get_mounts=True, mountpoint and mounted are also retrieved automatically
    # unlocking some functionality
    # To see all available properties use: % zfs list -o (-or-) % zpool list -o
    poolset = conn.load_poolset()

    # Load a pool by name
    pool = poolset.get_pool('dpool')
    
    # Get properties from ZFSItem's (Pool|DataSet|Snapshot)
    # <ZFSItem>.get_property('<property>')
    # -also-
    # <ZFSItem>.path -> str: Full path for item
    # <ZFSItem>.name -> str: Name of item
    # <ZFSItem>.creation -> datetime
    # <dataset>.mountpoint -> str
    # <dataset|snapshot>.pool -> Pool
    # <snapshot>.dataset -> DataSet
    # <dataset>.dspath -> str: Dataset path excluding pool name
    # <ZFSItem>.parent -> ZfsItem
    # <pool|dataset>.children -> list(of ZfsItem)
    # . Pools only contain DataSets
    # . Datasets can contain DataSets and/or Snapshots


    # Load dataset
    ds = pool.get_dataset('vcmain')

    # Load snapshots by with name of autosnap* that fall between 
    # the dates of 2020-12-20 and 2020-12-24
    snapshots = ds.find_snapshots({
        'name': 'autosnap*', 'date_from': '2020-12-20', 'date_to': '2020-12-24'
    })

    # Get all the changes file modification diffs for files that end with .py and .js 
    # excluding those in __pycache__ between the first and second snapshots
    diffs = ds.get_diffs(
         snapshots[0], snapshots[1]
        ,file_type='F', chg_type='M'
        ,include=['*.py', '*.js']
        ,ignore=['*_pycache_*']
    )

    # Load snapshots by with name of autosnap* in the last 12 hours 
    snapshots = ds.find_snapshots({'name': 'autosnap*', 'tdelta': '12H'})

    # Get Path to a file in the Snapshot folder (under mountpoint/.zfs/snapshots):
    find_path = '<path_to_some_local_file_in_ZFS>'
    (exists, snap_path) = snapshots[0].resolve_snap_path(find_path)
    if exists:
        print('snap_path: {}'.format(snap_path))
    else: # file did not exist at time of snapshot creation
        print('File not found in snapshot: {}'.format(find_path))

```

## Some Key Features
### `<Dataset>.find_snapshots(dict)`
```
    # find_snapshots(dict) - Query all snapshots in Dataset
    #  Options:
    #  - name: Snapshot name (wildcard supported)
    #  - contains: Path to resource (wildcard supported)
    #  - dt_from: datetime to start
    #  - tdelta: timedelta -or- string of nC where: n is an integer > 0 and C is one of y,m,d,H,M,S. Eg 5H = 5 Hours
    #  - dt_to: datetime to stop 
    #  - index: (bool) - Return list(tuple(of int, snapshot, dataset)) where int is the index in current snaphot listing for dataset
    #  Notes:
    #  - Date searching is any combination of:
    #      (dt_from --> dt_to) | (dt_from --> dt_from + tdelta) | (dt_to - tdelta --> dt_to) | (dt_from --> now)
```

### `<Dataset>.get_property(str)`
```
    # get_property(str) - Return zfs item or zpool property
    #  - use zfs_props or zpool_props to grab non-defaulted properties
```

### `<Dataset>.get_diffs()`
```
    # get_diffs() - Gets Diffs in snapshot or between snapshots (if snap_to is specified)
    #               If snap_to is not specified, diff is to working copy
    # snap_from - Left side of diff
    # snap_to - Right side of diff. If not specified, diff is to current working version
    # include - list of glob expressions to include (eg ['*_pycache_*'])
    # exclude - list of glob expressions to exclude (eg ['*_pycache_*'])
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

See `test.py` for more sample code


Credits: This code is based heavily on [zfs-tools by Rudd-O](https://github.com/Rudd-O/zfs-tools).