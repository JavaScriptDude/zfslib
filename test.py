# See: https://github.com/JavaScriptDude/zfslib

import zfslib
import fnmatch
import sys
from datetime import datetime, timedelta


def main(argv):
    pool_name = 'rpool'
    ds_name = 'devel'
    # ds_name = 'USERDATA/tquinn_qxli5l'

    # Read ZFS information from local computer
    # Change properties as needed
    conn = zfslib.ZFSConnection(host='localhost', properties=["name", "avail", "usedsnap", "usedds", "usedrefreserv", "usedchild", "creation"])

    # Load pool
    poolset = conn.get_poolset()
    pool = poolset.lookup(pool_name)
    

    # # Print all datasets test
    # print_all_datasets(pool)

    # Load dataset
    ds = pool.get_dataset(ds_name)

    # Load snapshots by name and date/time range
    # snapshots = ds.find_snapshots({'name': 'autosnap*', 'tdelta': timedelta(hours=8), 'dt_to': datetime.now()})
    # snapshots = ds.find_snapshots({'name': 'autoz*', 'tdelta': '7d', 'dt_to': datetime.now()})
    # snapshots = ds.find_snapshots({'name': 'autosnap*'})
    snapshots = ds.find_snapshots({})

    # Get Snapshot path
    # for snap in snapshots:
    #     sp = snap.get_snap_path()


    # # Print Diffs in snapshots
    print_diffs_test(ds, snapshots)

    # get_pool() test
    p = ds.get_pool()

    # get_connection() test
    c = snapshots[-1:][0].get_connection()


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
    allds = pool.get_all_datasets(with_depth=True)
    for (depth, ds) in allds:
        print("{} {} ({}) - [{}] - {}".format(' .'*depth, ds.name, ds.get_property('name'), ds.get_property('mountpoint'), ds.get_property('used')))

def print_diffs_test(ds: zfslib.Dataset, snapshots: list):
    for i, snap in enumerate(snapshots):
        
        if i > 0:
            diffs = ds.get_diffs(snap_last, snap, file_type='F', chg_type='M', include=['*.vb', '*.py', '*.js', '*.aspx'], ignore=['*.vscod*', '*_pycache_*', '*/_other/db/*'])
            for diff in diffs:
                try:
                    if diff.get_is_text(): # Get diff of any text files
                        print('{} - {}'.format(snap.name, diff))
                        p_left = diff.get_snap_path_left()
                        p_right = diff.get_snap_path_right()

                        # print('. path left: {}'.format(p_left))
                        # print('. path right: {}'.format(p_right))
                        (adds, rems, err) = diff.get_file_diff()
                        if not err is None:
                            print("Had Error: {}".format(err))
                        else:
                            if adds == 0 and rems == 0:
                                print("  . (No changes)")
                            else:
                                print("  . file changed. Lines changed: -{} +{}".format(rems, adds))
                                print('''  . meld diff: % meld "{}" "{}"'''.format(p_left, p_right))
                except PermissionError as ex:
                    print("Had error: {}".format(ex))
                
        snap_last = snap


if __name__ == '__main__':
    main(sys.argv[1:])


