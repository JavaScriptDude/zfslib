import unittest
from datetime import datetime, timedelta, date as dt_date
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

        with self.assertRaises(KeyError): poolset.get_pool('pool')
        with self.assertRaises(KeyError): poolset.get_pool('*pool')


    # Test PoolSet.lookup and properties: name
    def test_poolset_lookup_pool(self):
        pool = poolset.lookup('dpool')
        self.assertIsInstance(pool, zfs.Pool)
        self.assertEqual(pool.name, 'dpool')

        # Lookup should fail unless pool name is specified
        with self.assertRaises(KeyError): poolset.lookup('pool')
        with self.assertRaises(KeyError): poolset.lookup('*pool')


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
        with self.assertRaises(KeyError): poolset.lookup('bpool/*OOT')
        with self.assertRaises(KeyError): poolset.lookup('bpool/OOT')


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
            with self.assertRaises(KeyError): poolset.lookup('BOOT/ubuntu_n2qr5q@autozsys_68frge')
            with self.assertRaises(KeyError): poolset.lookup('autozsys_68frge')
            with self.assertRaises(KeyError): poolset.lookup('@autozsys_68frge')



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
            with self.assertRaises(KeyError): ds2.get_child('foo')
            with self.assertRaises(KeyError): ds3.get_child('bar')


            ds = poolset.lookup('dpool/other')
            self.assertIsInstance(ds, zfs.Dataset)
            s_ts='1608446351'
            self.assertEqual(ds.get_property('creation'), s_ts)
            self.assertEqual(ds.creation, dt_from_creation(s_ts))
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
        with self.assertRaises(KeyError): pool.get_dataset('foo')
        with self.assertRaises(KeyError): pool.get_dataset('ubuntu_n2qr5q/srv')
        with self.assertRaises(KeyError): pool.get_dataset('OOT/ubuntu_n2qr5q/srv')
        with self.assertRaises(KeyError): pool.get_dataset('*OOT/ubuntu_n2qr5q/srv')



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
        with self.assertRaises(AssertionError): ds.get_snapshots(flt=None)
        with self.assertRaises(AssertionError): ds.get_snapshots(index='test')


    def test_snapable_get_snapshot(self):
        for pool in poolset:
            self.assertIsInstance(pool, zfs.Pool)
            all_ds = pool.get_all_datasets(with_depth=True)

            for (depth, ds) in all_ds:
                snaps = ds.get_all_snapshots()
                for snap in snaps:
                    self.assertIsInstance(snap, zfs.Snapshot)
                    snap2 = snap.dataset.get_snapshot(snap.name)
                    self.assertIs(snap, snap2)

        # Negative tests
        ds = poolset.lookup('rpool/ROOT/ubuntu_n2qr5q/var/spool')
        with self.assertRaises(KeyError): ds.get_snapshot('*utozsys_pfofay')
        with self.assertRaises(KeyError): ds.get_snapshot('foobar')

    # Notes:
    # contains flag cannot be tested with static data. Must move to dynamic testing
    def test_snapable_find_snapshots(self):
        ds = poolset.lookup('rpool/USERDATA/jbloggs_jb327m')
        self.assertIsInstance(ds, zfs.Dataset)

        all_snaps = ds.get_all_snapshots()
        
        # Empty options
        snaps = ds.find_snapshots({})
        self.assertEqual(len(snaps), len(all_snaps))

        # Name only
        snaps = ds.find_snapshots({'name': '*zsys_w*'})
        self.assertEqual(len(snaps), 3)

        snaps = ds.find_snapshots({'name': '*'})
        self.assertEqual(len(snaps), len(all_snaps))

        # Name and dt_from
        snaps = ds.find_snapshots({'name': '*zsys_*e*'
            , 'dt_from': dt_from_creation('1609272411')})
        self.assertEqual(len(snaps), 2)

        # Name and tdelta and dt_to
        # . Note: tdelta alone cannot be used for static unit testing
        snaps = ds.find_snapshots({'name': '*zsys_*e*', 'tdelta': timedelta(hours=36)
            ,'dt_to': dt_from_creation('1609362247')})
        self.assertEqual(len(snaps), 5)
        snaps2 = ds.find_snapshots({'name': '*zsys_*e*', 'tdelta': '36H'
            ,'dt_to': dt_from_creation('1609362247')})
        self.assertEqual(len(snaps2), len(snaps))
        for i, snap in enumerate(snaps):
            self.assertIs(snap, snaps2[i])

        # Name, dt_from and tdelta
        snaps = ds.find_snapshots({'name': '*zsys_*w*'
            ,'dt_from': dt_from_creation('1608233673'), 'tdelta': timedelta(hours=48)})
        self.assertEqual(len(snaps), 5)
        snaps2 = ds.find_snapshots({'name': '*zsys_*w*'
            ,'dt_from': dt_from_creation('1608233673'), 'tdelta': '48H'})
        self.assertEqual(len(snaps2), len(snaps))
        for i, snap in enumerate(snaps):
            self.assertIs(snap, snaps2[i])

        # Name, dt_from and dt_to
        snaps = ds.find_snapshots({'name': '*zsys_*w*'
            ,'dt_from': dt_from_creation('1608233673')
            ,'dt_to': dt_from_creation('1608772856')})
        self.assertEqual(len(snaps), 6)

        # Name, dt_from and dt_to using date instead of datetime
        snaps = ds.find_snapshots({'name': '*zsys_*w*'
            ,'dt_from': dt_date(2020, 12, 17)
            ,'dt_to': dt_date(2020, 12, 23)})
        self.assertEqual(len(snaps), 5)

        for i, snap in enumerate(snaps):
            self.assertIsInstance(snap, zfs.Snapshot)

        # Name, dt_from and dt_to using date instead of datetime with index=True
        snaps = ds.find_snapshots({'name': '*zsys_*w*', "index": True
            ,'dt_from': dt_date(2020, 12, 17)
            ,'dt_to': dt_date(2020, 12, 23)})
        self.assertEqual(len(snaps), 5)
        self.assertIsInstance(snaps[0], tuple)
        self.assertEqual(len(snaps[0]), 2)
        self.assertIsInstance(snaps[0][0], int)
        self.assertIsInstance(snaps[0][1], zfs.Snapshot)

        with self.assertRaises(TypeError): snaps = ds.find_snapshots()
        with self.assertRaises(AssertionError): snaps = ds.find_snapshots({'name': True})
        with self.assertRaises(AssertionError): snaps = ds.find_snapshots({'dt_from': 'asdf'})
        with self.assertRaises(AssertionError): snaps = ds.find_snapshots({'dt_to': 'asdf'})
        with self.assertRaises(AssertionError): snaps = ds.find_snapshots({'tdelta': 10})
        with self.assertRaises(KeyError): snaps = ds.find_snapshots({'tdelta': '-10H'})
        with self.assertRaises(AssertionError): snaps = ds.find_snapshots({'index': 1})
        with self.assertRaises(KeyError): 
            snaps = ds.find_snapshots({'dt_to': dt_date(2020, 12, 20)
                                     , 'dt_from': dt_date(2020, 12, 21)})
        with self.assertRaises(KeyError): 
            snaps = ds.find_snapshots({'dt_to': dt_date(2020, 12, 21)
                                      ,'dt_from': dt_date(2020, 12, 20)
                                      ,'tdelta': "1H"})




    # TODO:
    # [.] - Write tests for Dataset.get_diffs
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