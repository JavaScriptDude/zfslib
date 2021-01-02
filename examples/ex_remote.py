# Basic example for accessing ZFS on remote machine
# Test requirements:
# python-magic
import sys
import zfslib as zfs
from zfslib_ex_common import *


def main(argv):
    # Log into remote computer 'freenas' as user 'root'
    # This assumes you configured ssh for user 'root' on computer 'freenas' to allow
    # local computer to authenticitate with its public key. 
    # In real world, its suggested to create a user specific for zfs operations like 'zfs'
    conn = zfs.Connection(host='root@freenas')

    # Load poolset
    poolset = conn.load_poolset()


    # Get first dataset of the first pool
    p = poolset.items[0]
    all_datasets = p.get_all_datasets()
    ds = all_datasets[0]
    print('ds: {}'.format(ds))


    # Get first snapshot in specific pool/dataset
    p = poolset.get_pool('freenas-boot')
    ds = p.get_dataset('ROOT/11.3-U5')
    all_snaps = ds.get_all_snapshots()
    if len(all_snaps) == 0:
        print('No snapshots found for dataset: {}'.format(ds))
    else:
        print('First Snapshot for dataset {}: {}'.format(ds, all_snaps[0]))


    if False:
        # Lookup snapshot by its name within poolset
        snap = poolset.lookup('freenas-boot/ROOT/11.3-U5@2020-01-30-19:49:13')
        print('poolset.lookup snap: {}'.format(snap))


    if False:
        # Lookup snapshot by its name within pool
        p = poolset.get_pool('freenas-boot')
        snap = p.lookup('ROOT/11.3-U5@2020-01-30-19:49:13')
        print('pool.lookup snap: {}'.format(snap))


    # Iterate through all pools and print all datasets
    if False:
        print("Pools and Datasets:")
        for p in poolset:
            print_all_datasets(p)



if __name__ == '__main__':
    main(sys.argv[1:])
    sys.exit(0)


