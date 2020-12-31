#########################################
# .: zfslib.py :.
# Libraries for reading data from zfs with Python
# .: Other :.
# Author: Timothy C. Quinn
# Home: https://github.com/JavaScriptDude/zfslib
# Licence: https://opensource.org/licenses/GPL-3.0
#########################################
# TODO:
# [.] Test cases where a dataset is not mounted and handle appropriately

import subprocess
import os
import fnmatch
import pathlib
import inspect
from collections import OrderedDict
from datetime import datetime, timedelta, date as dt_date


# ZFS connection classes
class Connection:
    host = None
    _poolset = None
    _dirty = True
    _trust = False
    _props_last = None

    def __init__(self, host="localhost", trust=False, sshcipher=None, identityfile=None, knownhostsfile=None, verbose=False):
        self.host = host
        self._trust = trust
        self._poolset = PoolSet(self)
        self.verbose = verbose
        self._pools_loaded = False
        if host in ['localhost','127.0.0.1']:
            self.command = []
        else:
            self.command = ["ssh","-o","BatchMode=yes","-a","-x"]
            if self._trust:
                self.command.extend(["-o","CheckHostIP=no"])
                self.command.extend(["-o","StrictHostKeyChecking=no"])
            if sshcipher != None:
                self.command.extend(["-c",sshcipher])
            if identityfile != None:
                self.command.extend(["-i",identityfile])
            if knownhostsfile != None:
                self.command.extend(["-o","UserKnownHostsFile=%s" % knownhostsfile])
            self.command.extend([self.host])


    # Data is cached unless force=True, snapshots have been made since last read
    # or properties is different
    def load_poolset(self, properties:list=[], get_mounts:bool=True, force:bool=False, _test_data:str=None):
        if force or self._dirty or not self._props_last == properties:
            self._poolset._load(properties=properties, get_mounts=get_mounts, _test_data=_test_data)
            self._dirty = False
            self._props_last = properties

        return self._poolset


    def snapshot_recursively(self,name,snapshotname,properties={}):
        plist = sum( map( lambda x: ['-o', '%s=%s' % x ], properties.items() ), [] )
        subprocess.check_call(self.command + ["zfs", "snapshot", "-r" ] + plist + [ "%s@%s" % (name, snapshotname)])
        self._dirty = True



class PoolSet(object):
    _pools = None
    items = property(lambda self: [self._pools[p] for p in self._pools if True])
    have_mounts = False

    def __init__(self, conn:Connection):
        self.connection=conn
        self._pools = {}

    def get_pool(self, name) -> 'Pool':
        p = self.lookup(name)
        assert isinstance(p, Pool), "Item passed is not a pool name. Got: %s" % p
        return p

    # Lookup any type in poolset including: Pool, Dataset, Snapshot
    # Eg. for snapshots: <pool>/<dataset_path>@<snapshot>
    def lookup(self, name):
        if "@" in name:
            path, snapshot = name.split("@")
        else:
            path = name
            snapshot = None

        if "/" not in path:
            try: ret = self._pools[path]
            except KeyError: raise KeyError("No such pool %s" % (name))
            if snapshot:
                try: ret = ret.get_snapshot(snapshot)
                except KeyError: raise KeyError("No such snapshot %s at %s" % (snapshot, ret.path))
        else:
            head, tail = path.split("/", 1)
            try: pool = self._pools[head]
            except KeyError: raise KeyError("No such pool %s" % (head))
            if snapshot: tail = tail + "@" + snapshot
            ret = pool.lookup(tail)

        return ret
    
    # This is here only for legacy testing capability
    def parse_zfs_r_output(self, zfs_r_output, properties = None):
        self._load(get_mounts=False, properties=properties, _test_data=zfs_r_output)

    # Note: _test_data is for testing only
    # get_mounts will automated grabbing of mountpoint and mounted properties and
    # store flag for downstream code to know that these flags are available
    def _load(self, get_mounts:bool=True, properties:list=None, _test_data:str=None):

        _pdef=['name', 'creation']

        if properties is None:
            if get_mounts:
                _pdef.extend(['mountpoint', 'mounted'])
                self.have_mounts = True
            properties = _pdef

        else:
            if 'mountpoint' in properties and 'mounted' in properties:
                self.have_mounts = True

            elif get_mounts:
                _pdef.extend(['mountpoint', 'mounted'])
                self.have_mounts = True

            else:
                self.have_mounts = False

            properties = _pdef + [s for s in properties if not s in _pdef]

        _base_cmd = self.connection.command

        def extract_properties(s):
            s = s.decode('utf-8') if isinstance(s, bytes) else s
            items = s.strip().split( '\t' )
            assert len( items ) == len( properties ), (properties, items)
            propvalues = map( lambda x: None if x == '-' else x, items[ 1: ] )
            return [ items[ 0 ], zip( properties[ 1: ], propvalues ) ]

        if _test_data is None:
            zfs_list_output = subprocess.check_output(self.connection.command + ["zfs", "list", "-Hpr", "-o", ",".join( properties ), "-t", "all"])

        else: # Use test data
            zfs_list_output = _test_data

        creations = OrderedDict([ extract_properties( s ) for s in zfs_list_output.splitlines() if s.strip() ])

        # names of pools
        old_dsets = [ x.path for x in self.walk() ]
        old_dsets.reverse()
        new_dsets = creations.keys()
        pool_cur = None

        for dset in new_dsets:
            if "@" in dset:
                dset, snapshot = dset.split("@")
            else:
                snapshot = None
            if "/" not in dset:  # pool name
                if dset not in self._pools:
                    pool_cur = Pool(dset, self.connection, self.have_mounts)
                    self._pools[dset] = pool_cur
                    fs = self._pools[dset]
            poolname, pathcomponents = dset.split("/")[0], dset.split("/")[1:]
            fs = self._pools[poolname]
            for pcomp in pathcomponents:
                # traverse the child hierarchy or create if that fails
                try: fs = fs.get_child(pcomp)
                except KeyError:
                    fs = Dataset(pool_cur, pcomp, fs)

            if snapshot:
                if snapshot not in [ x.name for x in fs.children ]:
                    fs = Snapshot(pool_cur, snapshot, fs)

            fs._properties.update( creations[fs.path] )
            
            # std_avail is avail, std_ref is usedds


        for dset in old_dsets:
            if dset not in new_dsets:
                if "/" not in dset and "@" not in dset:  # a pool
                    self.remove(dset)
                else:
                    d = self.lookup(dset)
                    d.parent.remove(d)


    def remove(self, name):  # takes a NAME, unlike the child that is taken in the remove of the dataset method
        for c in self._pools[name].children:
            self._pools[name].remove(c)
        self._pools[name].invalidated = True
        del self._pools[name]


    def __getitem__(self, name):
        return self._pools[name]


    def __str__(self):
        return "<PoolSet at %s>" % id(self)
    __repr__ = __str__


    def walk(self):
        for item in self._pools.values():
            for dset in item.walk():
                yield dset

    def __iter__(self):
        for pool in self._pools:
            yield self._pools[pool]


# Wrappers for testing
class TestPoolSet(PoolSet):
    def __init__(self):
        self.connection=TestConnection()
        self._pools = {}

class TestConnection(Connection):
    def __init__(self):
        self.command=[]



# ZFSItem is an 'abstract' class for Pool, Dataset and Snapshot
class ZFSItem(object):
    name = None
    children = None
    _properties = None
    parent = None
    invalidated = False

    creation = property(lambda self: datetime.fromtimestamp(int(self._properties["creation"])))

    def __init__(self, pool, name, parent=None):
        self.pool = pool
        self.name = name
        self.children = []
        self._properties = {}
        if parent:
            self.parent = parent
            self.parent._add_child(self)

    def _add_child(self, child):
        self.children.append(child)
        return child


    def get_child(self, name):
        child = [ c for c in self.children if c.name == name ]
        assert len(child) < 2
        if not child: raise KeyError(name)
        return child[0]
        

    def remove(self, child):
        if child not in self.children: raise KeyError(child.name)
        child.invalidated = True
        child.parent = None
        self.children.remove(child)
        for c in child.children:
            child.remove(c)


    def walk(self):
        assert not self.invalidated, "%s invalidated" % self
        yield self
        for c in self.children:
            for element in c.walk():
                yield element

    def __iter__(self):
        return self.walk()

    def get_property(self, name):
        return self._properties[ name ]

    def has_property(self, name):
        return (name in self._properties)
    
    


class Snapable(ZFSItem): # Abstract class for Pools and Datasets

    def __init__(self, pool, name, parent=None):
        super().__init__(pool, name, parent)
        
    # Lookup for Datasets or Snapshot by dataset relative path
    # Eg. for snapshots: <dataset_path>@<snapshot>
    def lookup(self, name):
        if "@" in name:
            path, snapshot = name.split("@")
        else:
            path = name
            snapshot = None

        if "/" not in path:
            try: ret = self.get_child(path)
            except KeyError: raise KeyError("No such dataset %s under %s" % (path, self.path))
            if snapshot:
                try: ret = ret.get_snapshot(snapshot)
                except KeyError: raise KeyError("No such snapshot %s under %s" % (snapshot, ret.path))
        else:
            head, tail = path.split("/", 1)
            try: child = self.get_child(head)
            except KeyError: raise KeyError("No such dataset %s under %s" % (head, self.path))
            if snapshot: tail = tail + "@" + snapshot
            ret = child.lookup(tail)

        return ret
        

    def _get_path(self):
        if not self.parent: return self.name
        return "%s/%s" % (self.parent.path, self.name)

    path = property(_get_path)


    # For name, use full dataset path
    def get_dataset(self, name):
        assert not(isinstance(self, Snapshot)), "get_dataset(name) cannot be used on Snapshot Objects. Use Snapshot.dataset instead."
        allds = self.get_all_datasets()
        pool_name = self.name if isinstance(self, Pool) else self.pool.name
        nfind = name if name.find(pool_name+'/') == 0 else '{}/{}'.format(pool_name, name)
        for dataset in allds:
            if dataset.path == nfind:
                return dataset
        raise KeyError("Dataset '{}' not found in pool '{}'.".format(name, self.pool.name))
        

    # returns list(of str) or if with_depth == True then list(of tuple(of depth, Dataset))
    def get_all_datasets(self, with_depth:bool=False, depth:int=0):
        a = []
        for c in self.children:
            if isinstance(c, Dataset):
                a.append( (depth, c) if with_depth else c )
                a = a + c.get_all_datasets(with_depth=with_depth, depth=depth+1)
        return a


    # if index is True return list(of tuple(int, Snapshot))
    def get_snapshots(self, flt=True, index=False):
        if flt is True: flt = lambda _:True
        assert inspect.isfunction(flt), "flt must either be True or a Function. Got: {}".format(type(flt))
        assert isinstance(index, bool), "index must be a boolean. Got: {}".format(type(index))
        _ds_path = self.path
        res = []
        for idx, c in enumerate(self.children):
            if isinstance(c, Snapshot) and flt(c):
                res.append( (idx, c) if index else c )

        return res


    def get_all_snapshots(self):
        return self.get_snapshots()


    def get_snapshot(self, name):
        children = [ c for c in self.get_snapshots() if c.name == name ]
        assert len(children) < 2
        if not children: raise KeyError(name)
        return children[0]


    # find_snapshots(dict) - Query all snapshots in Dataset
    #  Options:
    #  - name: Snapshot name (wildcard supported)
    #  - contains: Path to resource (wildcard supported)
    #  - dt_from: datetime to start
    #  - tdelta: timedelta -or- string of nC where: n is an integer > 0 and C is one of y,m,d,H,M,S. Eg 5H = 5 Hours
    #  - dt_to: datetime to stop 
    #  - index: (bool) 
    #  Return: 
    #  -  list(tuple(of int, snapshot)) where int is the index in current snaphot listing for dataset
    #  Notes:
    #  - Date searching is any combination of:
    #      (dt_from --> dt_to) | (dt_from --> dt_from + tdelta) | (dt_to - tdelta --> dt_to) | (dt_from --> now)
    def find_snapshots(self, find_opts:dict) -> list:

        def __assert(k, types, default=None, to_datetime=False):
            if k == 'find_opts':
                v=find_opts
            else:
                if not k in find_opts: return default
                v = find_opts[k]
            assert isinstance(v, types), 'Invalid type for param {}. Expecting {} but got: {}'.format(k, types, type(v))

            if to_datetime and not isinstance(v, datetime):
                return datetime(v.year, v.month, v.day)
            return v

        find_opts = {} if find_opts is None else __assert('find_opts', (dict))
        name = __assert('name', (str))
        dt_from = __assert('dt_from', (datetime, dt_date), None, True)
        dt_to = __assert('dt_to', (datetime, dt_date), None, True)
        tdelta = __assert('tdelta', (str, timedelta))
        index = __assert('index', (bool), False)
        contains = __assert('contains', (str))

        if not contains is None:
            if not os.path.exists(contains):
                raise KeyError("Path in contains option does not exist: {}".format(contains))
            contains = self.get_rel_path(contains)
        
        f = dt_f = dt_t = None
        def __fil_n(snap):
            if not contains is None:
                if not os.path.exists('{}{}'.format(snap.snap_path, contains)): return False
            if not name is None and not fnmatch.fnmatch(snap.name, name): return False
            return True

        def __fil_dt(snap):
            if not __fil_n(snap): return False
            cdate = snap.creation
            if cdate < dt_f: return False
            if cdate > dt_t: return False
            return True

        
        if not dt_from and not dt_to and not tdelta:
            f = __fil_n

        elif not dt_from is None and dt_to is None and tdelta is None:
            (dt_f, dt_t) = (dt_from, datetime.now())
            f=__fil_dt

        elif not tdelta is None and dt_from is None and dt_to is None:
            tdelta = tdelta if isinstance(tdelta, timedelta) else buildTimedelta(tdelta)
            (dt_f, dt_t) = (datetime.now() - tdelta, datetime.now())
            f=__fil_dt

        elif not dt_from is None and not dt_to is None:
            if not tdelta is None:
                raise KeyError("tdelta cannot be specified when both dt_from and dt_to are specified")
            if dt_from >= dt_to:
                raise KeyError("dt_from ({}) must be < dt_to ({})".format(dt_from, dt_to))
            (dt_f, dt_t) = (dt_from, dt_to)
            f=__fil_dt

        else:
            f=__fil_dt
            if dt_from and dt_to and not tdelta:
                dt_f = dt_from
                dt_t = dt_to
            else:
                (dt_f, dt_t) = calcDateRange(tdelta=tdelta, dt_from=dt_from, dt_to=dt_to)
        

        return self.get_snapshots(flt=f, index=index)








class Pool(Snapable):
    def __init__(self, name, conn:Connection, have_mounts:bool):
        super().__init__(self, name)
        self.connection = conn
        self.have_mounts = have_mounts
        self.pool = self

        
    def __str__(self):
        return "<Pool:     %s>" % self.path

    __repr__ = __str__



class Dataset(Snapable):

    dspath=None
    _mountpoint=None
    _mounted=None
    
    def __init__(self, pool, name, parent=None):
        super().__init__(pool, name, parent)
        self.dspath = self.path[len(pool.name)+1:]


    # get_diffs() - Gets Diffs in snapshot or between snapshots (if snap_to is specified)
    # snap_from - Left side of diff
    # snap_to - Right side of diff. If not specified, diff is to working copy
    # include - list of glob expressions to include (eg ['*_pycache_*'])
    # exclude - list of glob expressions to exclude (eg ['*_pycache_*'])
    # file_type - Filter on the following
    #  - B       Block device
    #  - C       Character device
    #  - /       Directory
    #  - >       Door
    #  - |       Named pipe
    #  - @       Symbolic link
    #  - P       Event port
    #  - =       Socket
    #  - F       Regular file
    # chg_type - Filter on the following:
    #  - -       The path has been removed
    #  - +       The path has been created
    #  - M       The path has been modified
    #  - R       The path has been renamed
    def get_diffs(self, snap_from, snap_to=None, include:list=None, exclude:list=None, file_type=None, chg_type=None) -> list:
        self.assertHaveMounts()
        if snap_from is None or not isinstance(snap_from, Snapshot):
            raise Exception("snap_from must be a Snapshot")
        if not snap_to is None and not isinstance(snap_to, Snapshot):
            raise Exception("snap_to must be a Snapshot")
        if not include is None and not isinstance(include, list):
            raise Exception("snapincludeto must be a list")
        if not exclude is None and not isinstance(exclude, list):
            raise Exception("exclude must be a list")

        def __tv(k, v):
            if v is None: return None
            if isinstance(v, str): return [v]
            if isinstance(v, list): return v
            raise KeyError("{} can only be a str or list. Got: {}".format(k, type(v)))
        file_type = __tv('file_type', file_type)
        chg_type = __tv('chg_type', chg_type)

        cmd = self.pool.connection.command + ["zfs", "diff", "-FHt", snap_from.path]
        if snap_to:
            cmd = cmd + [snap_to.path]
            snap_left = snap_from
            snap_right = snap_to
        else:
            snap_left = snap_from
            snap_right = '(present)'


        stdout = subprocess.check_output(cmd)
        def __row(s):
            s = s.decode('utf-8') if isinstance(s, bytes) else s
            return s.strip().split( '\t' )
        rows = list(map(lambda s: __row(s), stdout.splitlines()))
        diffs = []
        for row in rows:
            d = Diff(row, snap_left, snap_right)
            if not file_type is None and not d.file_type in file_type: continue
            if not chg_type is None and not d.chg_type in chg_type: continue

            if not include is None:
                bOk=False
                for incl in include:
                    if fnmatch.fnmatch(d.path_full, incl) \
                        or (not d.path_r is None and fnmatch.fnmatch(d.path_r_full, incl)):
                        bOk = True
                        break
                if not bOk: continue


            if not exclude is None:
                bIgn = False
                for excl in exclude:
                    if fnmatch.fnmatch(d.path_full, excl) \
                        or (not d.path_r is None and fnmatch.fnmatch(d.path_r_full, excl)):
                        bIgn = True
                        break
                if bIgn: continue
            diffs.append(d)

        return diffs


    def _get_mountpoint(self):
        if self._mountpoint is None:
            self.assertHaveMounts()
            self._mountpoint = self.get_property('mountpoint')
        return self._mountpoint
    mountpoint = property(_get_mountpoint)


    def _get_mounted(self):
        if self._mounted is None:
            self.assertHaveMounts()
            self._mounted = True if self.get_property('mounted') == 'yes' else False
        return self._mounted
    mounted = property(_get_mounted)

    has_mount = property(lambda self: False if self.mountpoint == 'none' else True)

    # def _get_path(self):
    #     if not self.parent: return self.name
    #     return "%s/%s" % (self.parent.path, self.name)


    # Return relative path to resource within a dataset
    # path must be an actual path on the system being analyzed
    def get_rel_path(self, path) -> str:
        self.assertHaveMounts()
        p_real = os.path.abspath( pathlib.Path(path).expanduser() )
        p_real = os.path.realpath(p_real)
        mp = self.mountpoint
        if not p_real.find(mp) == 0:
            raise KeyError('path given is not in current dataset mountpoint {}. Path: {}'.format(mp, path))
        return p_real.replace(mp, '')


    def assertHaveMounts(self):
        assert self.pool.have_mounts, "Mount information not loaded. Please use Connection.load_poolset(get_mounts=True)."


    def __str__(self):
        if self.pool.have_mounts:
            return "<Dataset:  %s> mountpoint: %s" % (self.path, self.mountpoint)
        else:
            return "<Dataset:  %s>" % (self.path)

    __repr__ = __str__





class Snapshot(ZFSItem):

    def __init__(self, pool, name, parent=None):
        super().__init__(pool, name, parent)
        self.dataset = parent if isinstance(parent, Dataset) else None


    def _get_path(self):
        if not self.parent: return self.name
        return "%s@%s" % (self.parent.path, self.name)
    path = property(_get_path)


    # Resolves the path to .zfs/snapshot directory
    def get_snap_path(self):
        self.parent.assertHaveMounts()
        return "{}/.zfs/snapshot/{}".format(self.parent.mountpoint, self.name)
    snap_path = property(get_snap_path)

    


    # Resolves the path to itm within the .zfs/snapshot directory
    # Returns: tuple(of bool, str) where:
    # - bool = True if item is found
    # - str = Path to item if found else path to .zfs/snapshot directory
    # eg: (found, rel_path) = snap.resolve_snap_path('<some_path_on_system>')
    def resolve_snap_path(self, path):
        self.parent.assertHaveMounts()
        if path is None or not isinstance(path, str) or path.strip() == '':
            assert 0, "path must be a non-blank string"
        path = os.path.abspath( pathlib.Path(path).expanduser() )
        path_real = os.path.realpath(path)
        snap_path_base = self.snap_path
        ds_mp = self.dataset.mountpoint
        if path_real.find(ds_mp) == -1:
            raise KeyError("Path given is not within the dataset's mountpoint of {}. Path passed: {}".format(ds_mp, path))
        snap_path = "{}{}".format(snap_path_base, path_real.replace(ds_mp, ''))
        if os.path.exists(snap_path):
            return (True, snap_path)
        else:
            return (False, snap_path_base)


    def __str__(self):
        return "<Snapshot: %s>" % self.path
    __repr__ = __str__


    # Legacy Shims
    def get_path(self):
        return self._get_path()








class Diff():
    def __init__(self, row:list, snap_left, snap_right):
        self.no_from_snap=False
        self.to_present=False
        if isinstance(snap_left, str) and snap_left == '(na-first)':
            self.no_from_snap=True
            snap_left = None
        elif not isinstance(snap_left, Snapshot):
            raise KeyError("snap_left must be either a Snapshot or str('na-first'). Got: {}".format(type(snap_left)))

        if isinstance(snap_right, str) and snap_right == '(present)':
            self.to_present=True
            snap_right = None

        elif not isinstance(snap_right, Snapshot):
            raise KeyError("snap_left must be either a Snapshot. Got: {}".format(type(snap_right)))

        if not self.no_from_snap and not self.to_present and snap_left.creation >= snap_right.creation:
            raise KeyError("diff from creation ({}) is > or = to diff_to creation ({})".format(snap_left.creation, snap_right.creation))

        self.snap_left = snap_left
        self.snap_right = snap_right

        if len(row) == 4:
            (inode_ts, chg_type, file_type, path_l) = row
            path_r = None
        elif len(row) == 5:
            (inode_ts, chg_type, file_type, path_l, path_r) = row
        else:
            raise Exception("Unexpected len: {}. Row = {}".format(len(row), row))

        chg_time = datetime.fromtimestamp(int(inode_ts[:inode_ts.find('.')]))
        self.chg_ts = inode_ts
        self.chg_time = chg_time
        self.chg_type = chg_type
        self.file_type = file_type
        if file_type == '/':
            self.file = None
            self.path = path_l
            self.path_full = path_l
        else:
            (f_l, p_l) = splitPath(path_l)
            self.file = f_l
            self.path = p_l
            self.path_full = path_l

        if file_type == '/':
            self.file_r = None
            self.path_r = path_r
            self.path_r_full = path_r
        else:
            (f_r, p_r) = splitPath(path_r) if not path_r is None else (None, None)
            self.file_r = f_r
            self.path_r = p_r
            self.path_r_full = path_r


    # Resolves path to resource on left side of diff in zfs_snapshot dir
    def _get_snap_path_left(self):
        if self.no_from_snap:
            raise Exception("Diff does not have a left snapshot because it is the first one. You can check using the no_from_snap property")
        snap_path = self.snap_left.snap_path
        return "{}{}".format(snap_path, self.path_full.replace(self.snap_left.dataset.mountpoint, ''))
    snap_path_left = property(_get_snap_path_left)


    # Resolves path to resource on right side of diff in zfs_snapshot dir or working copy
    def _get_snap_path_right(self):
        if self.to_present:
            return self.path_full
        snap_path = self.snap_right.snap_path
        return "{}{}".format(snap_path, self.path_full.replace(self.snap_left.dataset.mountpoint, ''))
    snap_path_right = property(_get_snap_path_right)


    def __str__(self):
        return "<Diff> {0} [{1}][{2}] {3}{4}".format(
            self.chg_time.strftime("%Y-%m-%d %H:%M:%S")
            ,self.chg_type, self.file_type
            ,self.path_full, ('' if not self.path_r_full else ' --> '+self.path_r))
    __repr__ = __str__



# Will resolve Pool and Dataset for a path on local filesystem using the mountpoint
# returns (Pool, Dataset, Real_Path, Relative_Path)
def find_dataset_for_path(poolset:PoolSet, path:str) -> tuple:
    assert poolset.have_mounts, "Mount information not loaded. Please use Connection.load_poolset(get_mounts=True)."
    p_real = os.path.abspath( pathlib.Path(path).expanduser() )
    p_real = os.path.realpath(p_real)
    pool=ds=mp=p_rela=None
    for pool_c in poolset:
        datasets = pool_c.get_all_datasets()
        for ds_c in datasets:
            mp_c = ds_c.mountpoint
            if p_real.find(mp_c) == 0:
                if mp is None or len(mp_c) > len(mp):
                    p_rela = p_real.replace(mp_c, '')
                    ds = ds_c
                    pool = pool_c
                    mp = mp_c
                    
    return (pool, ds, p_real, p_rela)


# buildTimedelta()
# Builds timedelta from string:
# . tdelta is a timedelta -or- str(nC) where: n is an integer > 0 and C is one of:
#   . y=year, m=month, w=week, d=day, H=hour, M=minute, s=second
# Note: month and year are imprecise and assume 30.4 and 365 days
def buildTimedelta(tdelta:str) -> timedelta:
    
    if not isinstance(tdelta, str):
        raise KeyError('tdelta must be a string')
    elif len(tdelta) < 2:
        raise KeyError('len(tdelta) must be >= 2')
    n = tdelta[:-1]
    try:
        n = int(n)
        if n < 1: raise KeyError('tdelta must be > 0')
    except ValueError as ex:
        raise KeyError('Value passed for tdelta does not contain a number: {}'.format(tdelta))
    
    c = tdelta[-1:]
    if c == 'H':
        return timedelta(hours=n)
    elif c == 'M':
        return timedelta(minutes=n)
    elif c == 'S':
        return timedelta(seconds=n)
    elif c == 'd':
        return timedelta(days=n)
    elif c == 'm':
        return timedelta(days=n*30.4)
    elif c == 'w':
        return timedelta(weeks=n)
    elif c == 'y':
        return timedelta(days=n*365)
    else:
        raise KeyError('Unexpected datetime identifier, expecting one of y,m,w,d,H,M,S.')


# calcDateRange()
# Calculates a date range based on tdelta string passed
# tdelta is a timedelta -or- str(nC) where: n is an integer > 0 and C is one of:
# . y=year, m=month, w=week, d=day, H=hour, M=minute, s=second
# If dt_from is defined, return tuple: (dt_from, dt_from+tdelta)
# If dt_to is defined, return tuple: (dt_from-tdelta, dt_to)
def calcDateRange(tdelta:str, dt_from:datetime=None, dt_to:datetime=None) -> tuple:
    if dt_from and dt_to:
        raise KeyError('Only one of dt_from or dt_to must be defined')
    elif (not dt_from and not dt_to):
        raise KeyError('Please specify one of dt_from or dt_to')
    elif tdelta is None:
        raise KeyError('tdelta is required')
    elif dt_from and not isinstance(dt_from, datetime):
        raise KeyError('dt_from must be  a datetime')
    elif dt_to and not isinstance(dt_to, datetime):
        raise KeyError('dt_to must be  a datetime')

    td = tdelta if isinstance(tdelta, timedelta) else buildTimedelta(tdelta)
    
    if dt_from:
        return (dt_from, (dt_from + td))
    else:
        return ((dt_to - td), dt_to)


def splitPath(s):
    f = os.path.basename(s)
    p = s[:-(len(f))-1]
    return f, p


