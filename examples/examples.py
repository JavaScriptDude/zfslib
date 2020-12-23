# See: https://github.com/JavaScriptDude/zfslib

import sys, magic, subprocess
import zfslib as zfs
from datetime import datetime, timedelta


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


def print_all_datasets(pool: zfs.Pool):
    allds = pool.get_all_datasets(with_depth=True)
    for (depth, ds) in allds:
        print("{}: {} {} ({}) - [{}] - {}".format(pool.name, ' .'*depth, ds.name, ds.name, ds.get_property('mountpoint'), ds.get_property('used')))


# This can be very slow for large datasets. Its actually `zfs diff` thats the slow part
def print_diffs_test(ds: zfs.Dataset, snapshots: list):
    snap_last = None
    for i, snap in enumerate(snapshots):
        
        if i > 0:
            diffs = ds.get_diffs(snap_last, snap, file_type='F', chg_type='M', include=['*.vb', '*.py', '*.js', '*.aspx'], exclude=['*.vscod*', '*_pycache_*', '*/_other/db/*'])
            for diff in diffs:
                try:
                    if file_is_text(diff.snap_path_left): # Get diff of any text files
                        print('{} - {}'.format(snap.name, diff))
                        p_left = diff.snap_path_left
                        p_right = diff.snap_path_right

                        # print('. path left: {}'.format(p_left))
                        # print('. path right: {}'.format(p_right))
                        (adds, rems, err) = get_file_diff(diff)
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



def get_file_diff(diff:zfs.Diff):
    if not diff.file_type == 'F':
        raise Exception('get_file_diff() is only available for files (file_type = F).')
    if not diff.chg_type == 'M':
        raise Exception('get_file_diff() is only available for modify changes (chg_type = M).')
    p_left = diff.snap_path_left
    p_right = diff.snap_path_right

    cmd = ['diff', '-ubwB', p_left, p_right]
    # print('''Running diff cmd: % diff -ubwB "{}" "{}"'''.format(p_left, p_right))

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    if p.returncode > 1:
        print("!!!!!! Warning. Return code !=0 `{}`: {}. Code: {}".format(cmd, stderr, p.returncode))
        return (0,0, stderr)

    elif len(stderr) > 0:
        # return code is > 1 (no error), but there is a message in stderr
        print("`{}` return code is 0 but had stderr msg: {}".format(cmd, stderr))

    if p.returncode == 0:
        return (0, 0, None)

    if stdout == None:
        print('WARNING - stdout is None!')
        return (0, 0, "stdout is None")

    elif len(stdout) == 0: 
        print('WARNING - len(stdout) = 0!')
        return (0, 0, "stdout len is 0")

    stdout = stdout.decode('utf-8')
    add = 0
    rem = 0
    for line in stdout.splitlines():
        c = line[0]
        if c == '+':
            add = add + 1
        elif c == '-':
            rem = rem + 1

    return (add, rem, None)


def file_is_text(path):
    f = magic.Magic(mime=True, uncompress=False)
    mime =  f.from_file(path)
    return (mime == 'text/plain')

if __name__ == '__main__':
    main(sys.argv[1:])


