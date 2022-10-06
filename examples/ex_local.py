# Basic example for accessing ZFS on local machine
# Test requirements:
# python-magic
import sys
import zfslib as zfs
from zfslib_ex_common import *


def main(argv):
    # Read ZFS information from local computer
    conn = zfs.Connection(host='localhost')

    # Load poolset
    poolset = conn.load_poolset()


    # Get first dataset of the first pool
    p = poolset.items[0]
    all_datasets = p.get_all_datasets()
    ds = all_datasets[0]
    print('ds: {}'.format(ds))


    # Get first snapshot in specific pool/dataset
    p = poolset.get_pool('dpool')
    ds = p.get_dataset('vcmain')
    all_snaps = ds.get_all_snapshots()
    
    if len(all_snaps) == 0:
        print('No snapshots found for dataset: {}'.format(ds))
    else:
        print('First Snapshot for dataset {}: {}'.format(ds, all_snaps[0]))


    # Find Snapshots of name autosnap* in the last 4 hours
    snapshots = ds.find_snapshots({'name': 'autosnap*', 'tdelta': '4h'})


    # Iterate through all pools and print all datasets
    if False:
        print("Pools and Datasets:")
        for p in poolset:
            print_all_datasets(p)


if __name__ == '__main__':
    main(sys.argv[1:])
    sys.exit(0)



