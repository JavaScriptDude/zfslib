import sys
import os
import subprocess
from datetime import datetime
import zfslib as zfs

def dt_from_creation(creation):
    return datetime.fromtimestamp(int(creation))

# Loads and validates test data
def load_test_data(alias, properties):
    fpath="./tests/{}.tsv".format(alias)
    assert os.path.isfile(fpath), "Test Data file {} not found.".format(fpath) 
    ret=[]
    rc = 0
    with open(fpath) as f: 
        while True:
            rc = rc + 1
            line = f.readline()
            if not line: 
                break
            row = line.split('\t')
            assert len(row) == len(properties) \
                ,"Row number {} in file {} has incorrect number of columns ({}) expecting {}.".format(
                    rc, fpath, len(row), len(properties)
                )
            ret.append(line)
    return '\n'.join(ret)


        


''' Testing Wrappers '''
class TestConnection(zfs.Connection):
    def __init__(self):
        self.command=[]


class TestPoolSet(zfs.PoolSet):
    def __init__(self):
        self.connection=TestConnection()
        self._pools = {}

    # This is here only for legacy testing capability
    def parse_zfs_r_output(self, zfs_r_output, properties = None):
        self._load(get_mounts=False, properties=properties, _test_data=zfs_r_output)
''' END Testing Wrappers '''