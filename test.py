# See: https://github.com/JavaScriptDude/zfslib

import zfslib
import fnmatch
import sys
from datetime import datetime, timedelta


def main(argv):
    pool_name = 'rpool'
    ds_name = 'devel'

    # Read ZFS information from local computer
    # Change properties as needed
    src_conn = zfslib.ZFSConnection(host='localhost', properties=["name", "avail", "usedsnap", "usedds", "usedrefreserv", "usedchild", "creation"])

    # Load pool
    pool = src_conn.pools.lookup(pool_name)

    # Print all datasets test
    print_all_datasets(pool)

    # Load dataset
    ds = pool.get_child(ds_name)

    # Load snapshots by name and date/time range
    snapshots = zfslib.get_snapshots(ds, name='autosnap*', dt_to=datetime.now(), tdelta=timedelta(hours=24))

    # Print Diffs in snapshots
    print_diffs_test(src_conn, snapshots)


    ds_name_full = f"{pool_name}/{ds_name}"

    # Print datasets creation date
    print(f"{ds_name_full} creation date: {ds.get_creation()}")

    # Grab Snapshots
    snapshots = ds.get_snapshots()


    # Get Snapshot Creation date
    print(f"{ds_name_full}@{snap.name} creation date: {ds.get_creation()}")

    # Read property from DataSet / Snapshot
    print(f"{ds_name_full}@{snap.name} usedsnap: {snap.get_property('usedsnap')}")


def print_all_datasets(pool: zfslib.Pool):
    print('Datasets:') 
    allds = pool.get_all_datasets()
    for (depth, ds) in allds:
        print("{} {} ({}) - [{}] - {}".format(' .'*depth, ds.name, ds.get_property('name'), ds.get_property('mountpoint'), ds.get_property('used')))

def print_diffs_test(conn:zfslib.ZFSConnection, snapshots: list):
    for snap in snapshots:
        diffs = zfslib.get_diffs(conn, snap_from=snap, ignore=['*.vscod*', '*_pycache_*'])
        for diff in diffs:
            print('{} - {}'.format(snap.name, diff))


if __name__ == '__main__':
    main(sys.argv[1:])
