import unittest
from datetime import datetime
import zfslib as zfs
from zfslib_test_tools import *

# TODO:
# [.] Write negative tests for get_mounts=False

# Load / init test data
properties = ['name', 'creation', 'used', 'available', 'referenced', 'mountpoint','mounted']

zlist_data = load_test_data('data_mounts', properties)
poolset = zfs.TestPoolSet()
poolset.parse_zfs_r_output(zlist_data, properties=properties)
pool_names = pool_names = [p.name for p in poolset if True]

class PoolSetTests(unittest.TestCase):

    def test_pool_names(self):
        self.assertEqual(pool_names, ['bpool', 'dpool', 'rpool'])


    def test_zfs_list_parsing(self):
        lines = zlist_data.splitlines()
        for line in lines:
            line = line.decode('utf-8') if isinstance(line, bytes) else line
            if line.strip() == '': continue
            row = line.split('\t')
            name = row[0]
            
            if name.find('@') > -1:
                snap = poolset.lookup(name)
                self.assertIsInstance(snap, zfs.Snapshot)
                if not snap.dataset is None:
                    self.assertIsInstance(snap.dataset, zfs.Dataset)
                self.assertIsInstance(snap.pool, zfs.Pool)
                self.assertIsInstance(snap.parent, (zfs.Dataset, zfs.Pool))
                if name.find('/') > -1:
                    pool_name = name[:name.find('/')]
                    ds_path = name[len(pool_name)+1:name.find('@')]
                else:
                    pool_name = name[:name.find('@')]
                    ds_path=None
                pool = poolset.get_pool(pool_name)
                self.assertIs(pool, snap.pool)

                if ds_path is None: # Snapshot of pool
                    self.assertIs(pool, snap.pool)
                else:
                    ds1 = poolset.lookup('{}/{}'.format(pool_name, ds_path))
                    ds2 = pool.lookup(ds_path)
                    self.assertIs(ds1, ds2)
                    self.assertIs(ds1, ds2)
                    self.assertIs(snap.parent, ds1)
                    

            elif name.find('/') > -1:
                ds = poolset.lookup(name)
                self.assertIsInstance(ds, zfs.Dataset)
                self.assertIsInstance(ds.pool, zfs.Pool)
                self.assertIsInstance(ds.parent, (zfs.Dataset, zfs.Pool))

                pool_name = name[:name.find('/')]
                ds_path = name[len(pool_name)+1:]
                pool = poolset.get_pool(pool_name)
                self.assertIsInstance(pool, zfs.Pool)
                ds2 = pool.lookup(ds_path)
                ds3 = pool.get_dataset(ds_path)
                self.assertIs(ds, ds2)
                self.assertIs(ds2, ds3)
                self.assertIs(ds.pool, pool)

            else:
                pool = poolset.lookup(name)
                self.assertIsInstance(pool, zfs.Pool)
                self.assertIs(pool, poolset.get_pool(name))
                


    def test_poolset_get_pool(self):
        for pname in pool_names:
            pool = poolset.get_pool(pname)
            self.assertIsInstance(pool, zfs.Pool)
            self.assertEqual(pool.name, pname)

        with self.assertRaises(KeyError):
            poolset.get_pool('pool')
            poolset.get_pool('*pool')


    # Test PoolSet.lookup and properties: name
    def test_poolset_lookup_pool(self):
        pool = poolset.lookup('dpool')
        self.assertIsInstance(pool, zfs.Pool)
        self.assertEqual(pool.name, 'dpool')

        # Lookup should fail unless pool name is specified
        with self.assertRaises(KeyError):
            poolset.lookup('pool')
            poolset.lookup('*pool')


    # Test PoolSet.lookup, ZFSItem.lookup and properties: name, pool, dataset, parent, path, dspath
    def test_poolset_lookup_dataset(self):
        for (pool_c, dspath_c) in [
             ('bpool','BOOT')
            ,('rpool','ROOT')
            ,('rpool','ROOT/ubuntu_n2qr5q')
            ,('rpool','ROOT/ubuntu_n2qr5q/usr')
            ,('rpool','ROOT/ubuntu_n2qr5q/usr/local')
            ]:
            ds_fullpath = '{}/{}'.format(pool_c, dspath_c)
            ds_name = dspath_c[dspath_c.rfind('/')+1:] if '/' in dspath_c else dspath_c
            ds = poolset.lookup(ds_fullpath)
            self.assertIsInstance(ds, zfs.Dataset)
            self.assertEqual(ds.name, ds_name)
            self.assertEqual(ds.path, ds_fullpath)
            self.assertEqual(ds.dspath, dspath_c)

            pool = ds.pool
            self.assertIsInstance(pool, zfs.Pool)
            self.assertEqual(pool.name, pool_c)
            self.assertEqual(pool, ds.parent.pool)

            
        ds = poolset.lookup('bpool/BOOT')
        self.assertIsInstance(ds.parent, zfs.Pool)

        # Lookup should fail unless full path to dataset is specified
        with self.assertRaises(KeyError):
            poolset.lookup('bpool/*OOT')
            poolset.lookup('bpool/OOT')


    # Test PoolSet.lookup, ZfsItem.lookup, ZfsItem.get_child, and properties: name, pool, dataset, parent, path
    def test_poolset_lookup_snapshot(self):
        for (pool_c, spath_c, cdate_c) in [
             ('bpool','BOOT/ubuntu_n2qr5q@autozsys_68frge', '1608162058')
            ,('bpool','BOOT/ubuntu_n2qr5q@autozsys_wo1pfb', '1608266786')
            ,('bpool','BOOT/ubuntu_n2qr5q@autozsys_fzhwyn', '1608401468')
            ]:
            snap_fullpath = '{}/{}'.format(pool_c, spath_c)
            snap_name = spath_c[spath_c.rfind('@')+1:]
            snap = poolset.lookup(snap_fullpath)
            self.assertIsInstance(snap, zfs.Snapshot)
            self.assertIsInstance(snap.parent, zfs.Dataset)
            dataset = snap.dataset
            self.assertIsInstance(dataset, zfs.Dataset)
            self.assertEqual(dataset.name, snap.parent.name)
            pool = snap.pool
            self.assertIsInstance(pool, zfs.Pool)
            self.assertEqual(pool.name, pool_c)
            self.assertEqual(pool, snap.parent.pool)

            self.assertEqual(snap.name, snap_name)
            self.assertEqual(snap.path, snap_fullpath)
            self.assertEqual(snap.get_property('creation'), cdate_c)
            
            # Lookup should fail unless full path to snapshot is specified
            with self.assertRaises(KeyError):
                poolset.lookup('BOOT/ubuntu_n2qr5q@autozsys_68frge')
                poolset.lookup('autozsys_68frge')
                poolset.lookup('@autozsys_68frge')



    def test_zfsitem_get_child_n_properties(self):

            pool = poolset.lookup('rpool')
            self.assertIsInstance(pool, zfs.Pool)
            ds = pool.lookup('ROOT')
            self.assertIsInstance(ds, zfs.Dataset)
            ds2=ds.get_child('ubuntu_n2qr5q')
            self.assertIsInstance(ds2, zfs.Dataset)
            self.assertEqual(ds2.get_property('used'), '12518498304')
            ds3=ds2.get_child('srv')
            self.assertIsInstance(ds3, zfs.Dataset)
            self.assertEqual(ds3.get_property('used'), '327680')
            snap = ds3.get_child('autozsys_j57yyo')
            self.assertIsInstance(snap, zfs.Snapshot)

            # Negative tests
            with self.assertRaises(KeyError):
                ds2.get_child('foo')
                ds3.get_child('bar')


            ds = poolset.lookup('dpool/other')
            self.assertIsInstance(ds, zfs.Dataset)
            s_ts='1608446351'
            self.assertEqual(ds.get_property('creation'), s_ts)
            self.assertEqual(ds.creation, datetime.fromtimestamp(int(s_ts)))
            self.assertEqual(ds.get_property('used'), '280644734976')
            self.assertEqual(ds.get_property('available'), '3564051083264')
            self.assertEqual(ds.get_property('referenced'), '266918285312')
            self.assertEqual(ds.mountpoint, '/dpool/other')
            self.assertEqual(ds.mounted, True)
            self.assertEqual(ds.has_mount, True)


            ds = poolset.lookup('bpool/BOOT')
            self.assertIsInstance(ds, zfs.Dataset)
            self.assertEqual(ds.get_property('creation'), '1608154061')
            self.assertEqual(ds.get_property('used'), '190410752')
            self.assertEqual(ds.get_property('available'), '1686265856')
            self.assertEqual(ds.get_property('referenced'), '98304')
            self.assertEqual(ds.mountpoint, 'none')
            self.assertEqual(ds.has_mount, False)
            self.assertEqual(ds.mounted, False)


    def test_pool_get_datasets_etc(self):
        ds_counts = {}
        ds_counts['bpool'] = 2
        ds_counts['dpool'] = 4
        ds_counts['rpool'] = 20
        for pool in poolset:
            self.assertIs(pool.name in ds_counts, True)
            ds_count = ds_counts[pool.name]
            self.assertIsInstance(pool, zfs.Pool)
            all_ds = pool.get_all_datasets()
            self.assertEqual(len(all_ds), ds_count)

            all_ds = pool.get_all_datasets(with_depth=True)
            self.assertEqual(len(all_ds), ds_count)
            t=all_ds[0]
            self.assertIsInstance(t, tuple)
            self.assertEqual(len(t), 2)
            self.assertIsInstance(t[0], int)
            self.assertIsInstance(t[1], zfs.Dataset)

            self.assertIsInstance(all_ds, list)
            seen={}
            for (depth, ds) in all_ds: 
                self.assertIsInstance(ds, zfs.Dataset)
                self.assertIs(ds.path in seen, False, 'Duplicate object returned in Pool.get_all_datasets(): {}'.format(ds.path))
                seen[ds.path] = True
                self.assertEqual(ds, poolset.lookup(ds.path))
                self.assertEqual(ds, pool.lookup(ds.dspath))
                self.assertEqual(ds, pool.get_dataset(ds.dspath))


        # Negative Tests
        pool = poolset.lookup('rpool')
        with self.assertRaises(KeyError):
            pool.get_dataset('foo')
            pool.get_dataset('ubuntu_n2qr5q/srv')
            pool.get_dataset('OOT/ubuntu_n2qr5q/srv')
            pool.get_dataset('*OOT/ubuntu_n2qr5q/srv')



    def test_snapable_get_snapshots(self):
        pool = poolset.lookup('rpool')
        ds = pool.get_dataset('USERDATA/jbloggs_jb327m')
        self.assertIsInstance(ds, zfs.Dataset)
        snaps_all = ds.get_all_snapshots()
        self.assertIs(len(snaps_all), 56)
        snaps = ds.get_snapshots(flt=lambda s: int(s.get_property('used')) >= 3000000 and int(s.get_property('used')) <= 6000000)
        self.assertIs(len(snaps), 4)
        snaps = ds.get_snapshots(flt=lambda s: int(s.get_property('used')) >= 6000000 and int(s.get_property('used')) <= 9000000)
        self.assertIs(len(snaps), 4)
        snaps = ds.get_snapshots(flt=lambda s: True)
        self.assertIs(len(snaps), len(snaps_all))
        snaps = ds.get_snapshots()
        self.assertIs(len(snaps), len(snaps_all))
        snaps = ds.get_snapshots(index=True)
        self.assertIs(len(snaps), len(snaps_all))

        snaps = ds.get_snapshots(flt=lambda s: int(s.get_property('used')) >= 3000000 and int(s.get_property('used')) <= 6000000, index=True)
        self.assertEqual(len(snaps), 4)
        self.assertIsInstance(snaps[1], tuple)
        self.assertEqual(len(snaps[1]), 2)
        self.assertIsInstance(snaps[1][0], int)
        self.assertIsInstance(snaps[1][1], zfs.Snapshot)
        self.assertEqual(snaps[1][0], 10)
        self.assertEqual(snaps[1][1].name, 'autozsys_wp6x2l')


        # Negative tests
        with self.assertRaises(AssertionError):
            ds.get_snapshots(flt=None)
            ds.get_snapshots(index='test')


    # TODO:
    # [.] - Write tests for Snapable.get_snapshot
    # [.] - Write tests for Snapable.find_snapshots
    # [.] - Write tests for Dataset.find_snapshots
    #       Not sure how to write this it needs dynamic data calls
    # [.] - Write tests for Dataset.get_rel_path
    # [.] - Write tests for Dataset.assertHaveMounts
    # [.] - Write tests for Snapshot.get_snap_path
    # [.] - Write tests for Snapshot.resolve_snap_path
    #       Note: may need refactoring to make it unit testable
    # [.] - Write tests for Diff class...
    # [.] - Write tests for find_dataset_for_path...
    #       Note: may need refactoring to make it unit testable


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()