import unittest
from datetime import datetime, timedelta, date as dt_date
import zfslib as zfs
from zfslib_test_tools import *

# ONLINE TESTING
# Assumes an ubuntu system with ZFS on root and 2 or more snapshots existing for /var/log
conn = zfs.Connection(host='localhost')

# Load poolset
poolset = conn.load_poolset(get_mounts=True, properties=['used', 'available', 'referenced'])

pool = poolset.get_pool('rpool')
ds_root = pool.get_dataset('ROOT')
ds_comp = None
for ds_c in ds_root.children:
    if isinstance(ds_c, zfs.Dataset) and ds_c.name.find('ubuntu_') == 0:
        ds_comp = ds_c
        break

ds_varlog = ds_comp.get_dataset('var/log')
snaps_varlog = ds_varlog.get_all_snapshots()


def validate_diff(diff):
    assert isinstance(diff, zfs.Diff)
    assert isinstance(zfs.Diff.get_file_type(diff.file_type), str)
    assert isinstance(zfs.Diff.get_change_type(diff.chg_type), str)

    def _checkPath(path):
        if diff.file_type == 'F':
            assert os.path.isfile(path)
        elif diff.file_type == '/':
            assert os.path.isdir(path)
        elif diff.file_type == '@':
            assert os.path.islink(path)
        else:
            assert os.path.exists(path)

    b_cl = b_cr = True
    if diff.chg_type == 'M':
        pass
    elif diff.chg_type == '-':
        b_cr = False
    elif diff.chg_type == '+':
        # Note. Truncations can show up as two diff records, one remove and one add so file may still exist
        b_cl = False
    elif diff.chg_type == 'R':
        pass
        
    if b_cl: _checkPath(diff.snap_path_left)
    if b_cr: _checkPath(diff.snap_path_right)


class Diff_Tests(unittest.TestCase):
    def setUp(self):
        if len(snaps_varlog) < 2:
            raise Exception("Testing cannot be done. Please ensure that Dataset rpool/ROOT/<computer>/var/log has more than one snapshot")

    def test_get_diffs(self):
        def _verify_diffs(self, step, diffs):
            self.assertIsInstance(diffs, list, "Step: {}. Object is not a list. Got: {}".format(step, type(diffs)))
            if len(diffs) == 0: return
            for diff in diffs:
                try:
                    validate_diff(diff)
                except Exception as ex:
                    raise AssertionError("During step {}. Had a validation issue with diff {}. Error: {}".format(step, diff, ex))


        snap_left = snaps_varlog[0]
        snap_right = snaps_varlog[-1:][0]

        # Single item and search by name
        diffs = ds_varlog.get_diffs(snap_from=snap_left, snap_to=snap_right
             ,include=['*apt/history.log'])
        _verify_diffs(self, "snap_left2snap_right_histlog", diffs)
        self.assertEqual(len(diffs), 1)

        # Get all diffs from first to last snapshot
        diffs_all = ds_varlog.get_diffs(snap_from=snap_left, snap_to=snap_right)
        _verify_diffs(self, "snap_left2snap_right", diffs_all)


        # Get all files ending with .log from beginning to end of snapshots
        diffs_log = ds_varlog.get_diffs(snap_from=snap_left, snap_to=snap_right
             ,include=['*.log'])
        _verify_diffs(self, "snap_left2snap_right_alllog", diffs_log)

        # Get all files ending with .gz from beginning to end of snapshots
        diffs_gz = ds_varlog.get_diffs(snap_from=snap_left, snap_to=snap_right
             ,include=['*.gz'])
        _verify_diffs(self, "snap_left2snap_right_allgz", diffs_gz)

        # Get all files ending with .gz from beginning to end of snapshots
        diffs_loggz = ds_varlog.get_diffs(snap_from=snap_left, snap_to=snap_right
             ,include=['*.log','*.gz'])
        _verify_diffs(self, "snap_left2snap_right_allloggz", diffs_loggz)

        # Get all diffs from first snapshot to current
        diffs = ds_varlog.get_diffs(snap_from=snap_left)
        _verify_diffs(self, "snap_left2Cur", diffs)


        # Test filter by file_type and change_type
        chg_t_counts={'+':0,'-':0,'M':0,'R':0}
        file_t_counts={'F':0,'/':0,'@':0}
        log_count=0
        gz_count=0
        for diff in diffs_all:
            chg_t_counts[diff.chg_type] = chg_t_counts[diff.chg_type] + 1
            if diff.file_type in file_t_counts:
                file_t_counts[diff.file_type] = file_t_counts[diff.file_type] + 1
            if diff.file_type == 'F':
                if len(diff.file) > 3 and diff.file[-4:] == '.log':
                    log_count = log_count + 1
                elif len(diff.file) > 2 and diff.file[-3:] == '.gz':
                    gz_count = gz_count + 1

        for chg_t in chg_t_counts:
            diffs = ds_varlog.get_diffs(snap_from=snap_left, snap_to=snap_right, chg_type=chg_t)
            _verify_diffs(self, "chg_type_check", diffs)
            self.assertEqual(len(diffs), chg_t_counts[chg_t])

        for file_t in file_t_counts:
            diffs = ds_varlog.get_diffs(snap_from=snap_left, snap_to=snap_right, file_type=file_t)
            _verify_diffs(self, "file_type_check", diffs)
            self.assertEqual(len(diffs), file_t_counts[file_t])

        # Test exclude of all log files
        # .log
        diffs = ds_varlog.get_diffs(snap_from=snap_left, snap_to=snap_right, exclude=['*.log'])
        _verify_diffs(self, "log_filter", diffs)
        self.assertEqual(len(diffs_all) - len(diffs), log_count)

        # .gz
        diffs = ds_varlog.get_diffs(snap_from=snap_left, snap_to=snap_right, exclude=['*.gz'])
        _verify_diffs(self, "gz_filter", diffs)
        self.assertEqual(len(diffs_all) - len(diffs), gz_count)

        # .gz and .log
        diffs = ds_varlog.get_diffs(snap_from=snap_left, snap_to=snap_right, exclude=['*.log','*.gz'])
        _verify_diffs(self, "log_gz_filter", diffs)
        self.assertEqual(len(diffs_all) - len(diffs), gz_count + log_count)

        self.assertEqual(len(diffs_log), log_count)
        self.assertEqual(len(diffs_gz), gz_count)
        self.assertEqual(len(diffs_loggz), log_count + gz_count)


        # Negative tests
        with self.assertRaises(TypeError): ds_varlog.get_diffs()
        with self.assertRaises(AssertionError): ds_varlog.get_diffs(snap_from='foo')
        with self.assertRaises(AssertionError): ds_varlog.get_diffs(snap_from=snap_left, snap_to='foo')
        with self.assertRaises(AssertionError): ds_varlog.get_diffs(snap_from=snap_left, snap_to=snap_right, exclude='foo')
        with self.assertRaises(AssertionError): ds_varlog.get_diffs(snap_from=snap_left, snap_to=snap_right, include='foo')
        with self.assertRaises(AssertionError): ds_varlog.get_diffs(snap_from=snap_left, snap_to=snap_right, file_type=1)
        with self.assertRaises(AssertionError): ds_varlog.get_diffs(snap_from=snap_left, snap_to=snap_right, chg_type=1)



class Poolset_Tests(unittest.TestCase):
    def test_find_dataset_for_path(self):
        path = '/var/log/apt/history.log'
        tup = poolset.find_dataset_for_path(path)
        self.assertIsInstance(tup, tuple)
        self.assertEqual(len(tup), 3)
        self.assertIsInstance(tup[0], zfs.Dataset)
        self.assertTrue(os.path.isfile(tup[1]))

        path = '/var/log/dmesg'
        tup = poolset.find_dataset_for_path(path)
        self.assertIsInstance(tup, tuple)
        self.assertEqual(len(tup), 3)
        self.assertIsInstance(tup[0], zfs.Dataset)
        self.assertTrue(os.path.isfile(tup[1]))

        path = '/var/log/apt'
        tup = poolset.find_dataset_for_path(path)
        self.assertIsInstance(tup, tuple)
        self.assertEqual(len(tup), 3)
        self.assertIsInstance(tup[0], zfs.Dataset)
        self.assertTrue(os.path.isdir(tup[1]))

        path = '~/../../var/log/apt'
        tup = poolset.find_dataset_for_path(path)
        self.assertIsInstance(tup, tuple)
        self.assertEqual(len(tup), 3)
        self.assertIsInstance(tup[0], zfs.Dataset)
        self.assertTrue(os.path.isdir(tup[1]))


        # Negative Tests
        path = '/foo/bar/baz/none.txt'
        tup = poolset.find_dataset_for_path(path)
        self.assertIsInstance(tup, tuple)
        self.assertEqual(tup[0], None)
        self.assertEqual(tup[1], '/foo/bar/baz/none.txt')
        self.assertEqual(tup[2], None)



class Dataset_Tests(unittest.TestCase):

    def test_get_rel_path(self):
        self.assertEqual(ds_varlog.get_rel_path('/var/log/apt/history.log'), '/apt/history.log')

        # Negative tests
        with self.assertRaises(AssertionError): ds_varlog.get_rel_path(1)
        with self.assertRaises(KeyError): ds_varlog.get_rel_path('foo_bar.baz')

    


class Snapshot_Tests(unittest.TestCase):

    def test_resolve_snap_path(self):
        snap = snaps_varlog[1]
        snap_path = snap.snap_path

        self.assertTrue(snap.resolve_snap_path('/var/log/apt/history.log')[0])
        self.assertTrue(os.path.isfile(snap.resolve_snap_path('/var/log/apt/history.log')[1]))
        self.assertEqual(snap.resolve_snap_path('/var/log/apt/history.log')[1].find(snap_path), 0)

        self.assertTrue(snap.resolve_snap_path('/var/log/apt/')[0])
        self.assertTrue(os.path.isdir(snap.resolve_snap_path('/var/log/apt/')[1]))
        self.assertEqual(snap.resolve_snap_path('/var/log/apt/')[1].find(snap_path), 0)

        self.assertTrue(snap.resolve_snap_path('/var/log/apt')[0])
        self.assertTrue(os.path.isdir(snap.resolve_snap_path('/var/log/apt')[1]))
        self.assertEqual(snap.resolve_snap_path('/var/log/apt')[1].find(snap_path), 0)

        # Negative tests
        with self.assertRaises(TypeError): snap.resolve_snap_path()
        with self.assertRaises(AssertionError): snap.resolve_snap_path(None)
        with self.assertRaises(AssertionError): snap.resolve_snap_path(1)
        with self.assertRaises(KeyError): snap.resolve_snap_path('foo_bar.baz')

   


if __name__ == "__main__":
    unittest.main()