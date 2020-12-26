import sys, magic, subprocess
import zfslib as zfs

from datetime import datetime, timedelta

def print_all_datasets(pool: zfs.Pool):
    allds = pool.get_all_datasets(with_depth=True)
    for (depth, ds) in allds:
        print("{}: {} {} ({}) - [{}] - {}".format(pool.name, ' .'*depth, ds.name, ds.name, ds.get_property('mountpoint'), ds.get_property('used')))



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