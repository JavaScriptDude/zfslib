# See: https://github.com/JavaScriptDude/zfslib

import sys, magic, subprocess
from datetime import datetime, timedelta
import zfslib as zfs
from zfslib_ex_common import *

def main(argv):
    pool_name = 'dpool'
    ds_name = 'vcmain'

    # Read ZFS information from local computer
    # Change properties as needed
    conn = zfs.Connection(host='localhost', properties=["avail", "usedsnap", "usedds", "usedrefreserv", "usedchild", "creation"])

    # Load pool
    poolset = conn.get_poolset()


    # Print all datasets test
    print("Pools and Datasets:")
    for p in poolset:
        print_all_datasets(p)


    # Load pool by name
    pool = poolset.lookup(pool_name)
    

    # Load dataset by name
    ds = pool.get_dataset(ds_name)

    # Load snapshots by name and date/time range
    # snapshots = ds.find_snapshots({'name': 'autosnap*', 'tdelta': timedelta(hours=8), 'dt_to': datetime.now()})
    snapshots = ds.find_snapshots({'name': 'autosnap*', 'tdelta': '1m', 'dt_to': datetime.now()})
    # snapshots = ds.find_snapshots({'name': 'autosnap*'})
    # snapshots = ds.find_snapshots({})

    # Get Snapshot path
    path_to_find = '/dpool/vcmain/py/zfs'
    for i, snap in enumerate(snapshots):
        if i > 0:
            diffs = ds.get_diffs(snap_last, snap, file_type='F', chg_type='M', include=['*.py', '*.js'], exclude=['*.vscod*', '*_pycache_*'])
            if len(diffs) > 0:
                sp = snap.snap_path
                (found, sp_res) = snap.resolve_snap_path(path_to_find)
                if found:
                    print("path not found: {}".format(path_to_find))
                else:
                    print("sp_res: {}".format(sp_res))

                break

        snap_last = snap


    # # Print Diffs in snapshots (can be slow)
    # print_diffs_test(ds, snapshots)

    if len(snapshots) > 0:
        # get connection from last snapshot
        c = snapshots[-1:][0].pool.connection


    ds_name_full = f"{pool_name}/{ds_name}"

    # Print datasets creation date
    print(f"{ds_name_full} creation date: {ds.creation}")

    # Grab Snapshots
    snapshots = ds.get_snapshots()

    if len(snapshots) > 0:
        snap=snapshots[0]

        # Get Snapshot Creation date
        print(f"{ds_name_full}@{snap.name} creation date: {ds.creation}")

        # Read property from DataSet / Snapshot
        print(f"{ds_name_full}@{snap.name} usedsnap: {snap.get_property('usedsnap')}")



if __name__ == '__main__':
    main(sys.argv[1:])


