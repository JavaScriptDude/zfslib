# Basic example for accessing ZFS on remote machine

import sys, magic, subprocess
from datetime import datetime, timedelta
import zfslib as zfs
from zfslib_ex_common import *


def main(argv):
    # Log into remote computer 'freenas' as user 'root'
    # This assumes you configured ssh for user 'root' on computer 'freenas' to allow
    # local computer to authenticitate with its public key. 
    # In real world, its suggested to create a user specific for zfs operations like 'zfs'
    conn = zfs.Connection(host='root@freenas', properties=["creation"])

    # Load pool
    poolset = conn.get_poolset()


    # Print all datasets test
    print("Pools and Datasets:")
    for p in poolset:
        print_all_datasets(p)



if __name__ == '__main__':
    main(sys.argv[1:])


