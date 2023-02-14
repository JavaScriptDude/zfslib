#########################################
# .: zfslib.py :.
# Libraries for reading data from zfs with Python
# .: Other :.
# Author: Timothy C. Quinn
# Home: https://github.com/JavaScriptDude/zfslib
# Licence: https://opensource.org/licenses/BSD-3-Clause
# TODO:
# [.] Allow querying of just zpool properties only rather than digging all zfs list -t all for every call
#     - This will make it much faster for such queries
#########################################

import subprocess
import os
import fnmatch
import pathlib
import inspect
from collections import OrderedDict
from datetime import datetime, timedelta, date as dt_date

class __DEFAULT__(object):pass

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


    
    # See PoolSet._load for parameters

    def load_poolset(self, zfs_props=None, zpool_props=None, get_mounts=True, force=False, _test_data_zfs=None, _test_data_zpool=None):
        zfs_props = [] if zfs_props is None else zfs_props
        if force or not self._props_last == zfs_props:
            self._poolset._load(zfs_props=zfs_props, zpool_props=zpool_props, get_mounts=get_mounts, _test_data_zfs=_test_data_zfs, _test_data_zpool=_test_data_zpool)
            self._props_last = zfs_props

        return self._poolset


class PoolSet(object):
    _pools = None
    items = property(lambda self: [self._pools[p] for p in self._pools if True])
    have_mounts = False

    def __init__(self, conn):
        self.connection=conn
        self._pools = {}

    def get_pool(self, name):
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
    
    # [zfs_props] properties from % zfs list -o <properties>
    # [zpool_props] properties from % zpool list -o <properties>
    # [get_mounts] Append  mountpoint and mounted zfs_props and store flag for downstream code to know that these flags are available
    # [_test_data_zfs] testing only
    # [_test_data_zpool] testing only
    def _load(self, get_mounts=True, zfs_props=None, zpool_props=None, _test_data_zfs=None, _test_data_zpool=None):

        # setup zfs list properties (zfs list -o <props>)
        _zfs_pdef=['name', 'creation']

        if zfs_props is None:
            if get_mounts:
                _zfs_pdef.extend(['mountpoint', 'mounted'])
                self.have_mounts = True
            zfs_props = _zfs_pdef

        else:
            if 'mountpoint' in zfs_props and 'mounted' in zfs_props:
                self.have_mounts = True

            elif get_mounts:
                _zfs_pdef.extend(['mountpoint', 'mounted'])
                self.have_mounts = True

            else:
                self.have_mounts = False

            zfs_props = _zfs_pdef + [s for s in zfs_props if not s in _zfs_pdef]



        # setup zpool list properties (zpool list -o <props>)
        _zpool_pdef=['name', 'size', 'allocated', 'free', 'checkpoint', 'fragmentation', 'capacity', 'health']
        if zpool_props is None:
            zpool_props = _zpool_pdef
        else:
            zpool_props = _zpool_pdef + [s for s in zpool_props if not s in _zpool_pdef]


        _base_cmd = self.connection.command

        def extract_properties(s, zpool:bool=False):
            props = zpool_props if zpool else zfs_props
            if isinstance(s, bytes): s = s.decode('utf-8')
            items = s.strip().split( '\t' )
            assert len( items ) == len( props ), (props, items)
            for i in range(1,len(props)):
                v = items[i]
                if v == '-':
                    items[i] = None
                elif props[i] in ZFS_INT_PROPS:
                    try:
                        items[i] = int(v)
                    except:
                        pass
                        
            return [ items[ 0 ], zip( props[ 1: ], items[ 1: ] ) ]

        # Gather zfs list data
        if _test_data_zfs is None:
            zfs_list_output = subprocess.check_output(self.connection.command + ["zfs", "list", "-Hpr", "-o", ",".join( zfs_props ), "-t", "all"])

        else: # Use test data
            zfs_list_output = _test_data_zfs

        zfs_list_items = OrderedDict([ extract_properties(s) for s in zfs_list_output.splitlines() if s.strip() ])


        # Gather zpool list data
        if _test_data_zpool is None:
            zpool_list_output = subprocess.check_output(self.connection.command + ["zpool", "list", "-Hp", "-o", ",".join( zpool_props )])

        else: # Use test data
            zpool_list_output = _test_data_zpool

        zpool_list_items = OrderedDict([ extract_properties(s, zpool=True) for s in zpool_list_output.splitlines() if s.strip() ])


        # names of pools
        old_items = [ x.path for x in self.walk() ]
        old_items.reverse()
        new_items = zfs_list_items.keys()
        pool_cur = None

        for name in new_items:
            is_zpool = False
            if "@" in name:
                name, snapshot = name.split("@")
            else:
                snapshot = None
            if "/" not in name:  # zpool
                if name not in self._pools:
                    is_zpool = True
                    pool_cur = Pool(name, self.connection, self.have_mounts)
                    self._pools[name] = pool_cur
                    fs = self._pools[name]

            poolname, pathcomponents = name.split("/")[0], name.split("/")[1:]
            fs = self._pools[poolname]
            for pcomp in pathcomponents:
                # traverse the child hierarchy or create if that fails
                try: fs = fs.get_child(pcomp)
                except KeyError:
                    fs = Dataset(pool_cur, pcomp, fs)

            if snapshot:
                if snapshot not in [ x.name for x in fs.children ]:
                    fs = Snapshot(pool_cur, snapshot, fs)

            fs._properties.update( zfs_list_items[fs.path] )

            if is_zpool:
                # Update with zpool properties
                _zpool_props = zpool_list_items.get(name, __DEFAULT__)
                assert _zpool_props != __DEFAULT__, f"ERROR - zpool '{name}' not found in zpool_list_items"
                fs._properties.update( _zpool_props )

            noop()
            
            # std_avail is avail, std_ref is usedds


        for name in old_items:
            if name not in new_items:
                if "/" not in name and "@" not in name:  # a pool
                    self.remove(name)
                else:
                    d = self.lookup(name)
                    d.parent.remove(d)


    def remove(self, name):  # takes a NAME, unlike the child that is taken in the remove of the dataset method
        for c in self._pools[name].children:
            self._pools[name].remove(c)
        self._pools[name].invalidated = True
        del self._pools[name]


    # Will resolve Pool and Dataset for a path on local filesystem using the mountpoint
    # returns (Pool, Dataset, Real_Path, Relative_Path)
    # Note: Ignores any dataset with root mountpoint (/)
    def find_dataset_for_path(self, path):
        assert self.have_mounts, "Mount information not loaded. Please use Connection.load_poolset(get_mounts=True)."
        p_real = os.path.abspath( expand_user(path) )
        p_real = os.path.realpath(p_real)
        pool=ds=mp=p_rela=None
        for pool_c in self:
            datasets = pool_c.get_all_datasets()
            for ds_c in datasets:
                if not ds_c.has_mount or ds_c.mountpoint == '/': continue
                mp_c = ds_c.mountpoint
                if p_real.find(mp_c) == 0:
                    if mp is None or len(mp_c) > len(mp):
                        p_rela = p_real.replace(mp_c, '')
                        ds = ds_c
                        pool = pool_c
                        mp = mp_c
                        
        return (ds, p_real, p_rela)


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





''' ZFS Entities

 Model:
    <ZFSItem> ----------------o         
    |                         |         
    v                         |         
    <Snapable> ---o           |         
    |             |           |         
    v             v           v         
    <Pool>       <Dataset>   <Snapshot> 
    -----------------------------------
    <Diff>

'''

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
        super(Snapable, self).__init__(pool, name, parent)
        
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
        ds = self.lookup(name)
        if isinstance(ds, Dataset): return ds

        if isinstance(self, Pool):
            raise KeyError(f"Dataset '{name}' not found in Pool '{self.name}'.")
        else:
            raise KeyError(f"Dataset '{name}' not found in Dataset '{self.path}'.")
        

    # returns list(of str) or if with_depth == True then list(of tuple(of depth, Dataset))
    def get_all_datasets(self, with_depth=False, depth=0):
        a = []
        for c in self.children:
            if isinstance(c, Dataset):
                a.append( (depth, c) if with_depth else c )
                a = a + c.get_all_datasets(with_depth=with_depth, depth=depth+1)
        return a


    # if index is True return list(of tuple(int, Snapshot))
    # WARNING - Using this function to filter if snapshot contains a folder
    def get_snapshots(self, flt=True, index=False):
        if flt is True: flt = lambda _:True
        assert inspect.isfunction(flt), f"flt must either be True or a Function. Got: {type(flt)}"
        assert isinstance(index, bool), f"index must be a boolean. Got: {type(index)}"
        _ds_path = self.path
        res = []
        for idx, c in enumerate(self.children):
            if isinstance(c, Snapshot) and flt(c):
                res.append( (idx, c) if index else c )

        return res


    def get_all_snapshots(self, index:bool=False):
        return self.get_snapshots(index=index)


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
    def find_snapshots(self, find_opts):

        def __assert(k, types, default=None, to_datetime=False):
            if k == 'find_opts':
                v=find_opts
            else:
                if not k in find_opts: return default
                v = find_opts[k]
            assert isinstance(v, types), f'Invalid type for param {k}. Expecting {types} but got: {type(v)}'

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
            # Removing - Path may no longer exists in regular FS but does exist in snapshot
            # if not os.path.exists(contains):
            #     raise KeyError("Path in contains option does not exist: {}".format(contains))
            contains = self.get_rel_path(contains)
        
        f = dt_f = dt_t = None
        def __fil_n(snap):
            if not contains is None:
                _check = f'{snap.snap_path}{contains}'
                # print(f"Checking: {_check}")
                if not os.path.exists(_check): return False
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
                raise AssertionError("tdelta cannot be specified when both dt_from and dt_to are specified")
            if dt_from >= dt_to:
                raise AssertionError(f"dt_from ({dt_from}) must be < dt_to ({dt_to})")
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
    def __init__(self, name, conn, have_mounts):
        super(Pool, self).__init__(self, name)
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
        super(Dataset, self).__init__(pool, name, parent)
        self.dspath = self.path[len(pool.name)+1:]


    # get_diffs() - Gets Diffs in snapshot or between snapshots (if snap_to is specified)
    # snap_from - Left side of diff
    # snap_to - Right side of diff. If not specified, diff is to working copy
    # include - list of glob expressions to include (eg ['*.py', '*.js'])
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
    def get_diffs(self, snap_from, snap_to=None, include=None, exclude=None, file_type=None, chg_type=None):
        self.assertHaveMounts()
        assert self.mounted, "Cannot get diffs for Unmounted Dataset. Verify mounted flag on Dataset before calling"

        if snap_from is None or not isinstance(snap_from, Snapshot):
            raise AssertionError("snap_from must be a Snapshot")
        if not snap_to is None and not isinstance(snap_to, Snapshot):
            raise AssertionError("snap_to must be a Snapshot")
        if not include is None and not isinstance(include, list):
            raise AssertionError("snapincludeto must be a list")
        if not exclude is None and not isinstance(exclude, list):
            raise AssertionError("exclude must be a list")

        def __tv(k, v):
            if v is None: return None
            if isinstance(v, str): return [v]
            if isinstance(v, list): return v
            raise AssertionError(f"{k} can only be a str or list. Got: {type(v)}")


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

        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
            stdout, stderr = p.communicate()
            stdout=stdout.decode('utf-8')
            stderr=stderr.decode('utf-8')
            if not p.returncode == 0:
                print(f"get_diffs() failed executing command '{cmd}': {stderr} ({p.returncode})")
                return []
        except Exception as exc:
            raise exc


        def __row(s):
            if isinstance(s, bytes): s = s.decode('utf-8')
            return s.strip().split( '\t' )

        rows = list(map(lambda s: __row(s), stdout.splitlines()))
        diffs = []
        for i, row in enumerate(rows):
            # if i == 429:
            #     print("HERE")
            d = Diff(row, snap_left, snap_right)
            if d.path_full.find('(on_delete_queue)') > 0:
                # It looks to be an artefact of ZFS that does not actually exist in FS
                # https://github.com/openzfs/zfs/blob/master/lib/libzfs/libzfs_diff.c
                continue
            if not file_type is None and not d.file_type in file_type: continue
            if not chg_type is None and not d.chg_type in chg_type: continue

            if not include is None:
                bOk=False
                for incl in include:
                    if fnmatch.fnmatch(d.path_full, incl) \
                        or (not d.path_new is None and fnmatch.fnmatch(d.path_full_new, incl)):
                        bOk = True
                        break
                if not bOk: continue


            if not exclude is None:
                bIgn = False
                for excl in exclude:
                    if fnmatch.fnmatch(d.path_full, excl) \
                        or (not d.path_new is None and fnmatch.fnmatch(d.path_full_new, excl)):
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


    # Return relative path to resource within a dataset
    # path must be an actual path on the system being analyzed
    def get_rel_path(self, path):
        self.assertHaveMounts()
        assert isinstance(path, str), f"argument passed is not a string. Got: {type(path)}"
        p_real = os.path.abspath( expand_user(path) )
        p_real = os.path.realpath(p_real)
        mp = self.mountpoint
        if not p_real.find(mp) == 0:
            raise KeyError(f'path given is not in current dataset mountpoint {mp}. Path: {path}')
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
        super(Snapshot, self).__init__(pool, name, parent)
        self.dataset = parent if isinstance(parent, Dataset) else None


    def _get_path(self):
        if not self.parent: return self.name
        return "%s@%s" % (self.parent.path, self.name)
    path = property(_get_path)


    def _get_name_full(self):
        if not self.parent: return self.name
        return "%s@%s" % (self.parent.dspath, self.name)
    name_full = property(_get_name_full)


    # Resolves the path to .zfs/snapshot directory
    def _get_snap_path(self):
        assert isinstance(self.parent, Dataset), \
            "This function is only available for Snapshots of Datasets not Pools"
        self.parent.assertHaveMounts()
        return f"{self.parent.mountpoint}/.zfs/snapshot/{self.name}"
    snap_path = property(_get_snap_path)

    


    # Resolves the path to file within the .zfs/snapshot directory
    # Returns: tuple(of bool, str) where:
    # - bool = True if item is found
    # - str = Path to item if found else path to .zfs/snapshot directory
    # eg: (found, rel_path) = snap.resolve_snap_path('<some_path_on_system>')
    def resolve_snap_path(self, path):
        assert isinstance(self.parent, Dataset), \
            "This function is only available for Snapshots of Datasets not Pools"
        self.parent.assertHaveMounts()
        assert self.parent.mounted, \
            f"Parent Dataset {self.parent} is not mounted. Please verify datsset.mounted before calling this function"

        if path is None or not isinstance(path, str) or path.strip() == '':
            assert 0, "path must be a non-blank string"
        path = os.path.abspath( expand_user(path) )
        path_neweal = os.path.realpath(path)
        snap_path_base = self.snap_path
        ds_mp = self.dataset.mountpoint
        if path_neweal.find(ds_mp) == -1:
            raise KeyError(f"Path given is not within the dataset's mountpoint of {ds_mp}. Path passed: {path}")
        snap_path = "{}{}".format(snap_path_base, path_neweal.replace(ds_mp, ''))
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

# END Snapshot




class Diff():
    FILE_TYPES={
         'B': 'Block device'
        ,'C': 'Character device'
        ,'/': 'Directory'
        ,'>': 'Door'
        ,'|': 'Named pipe'
        ,'@': 'Symbolic link'
        ,'P': 'Event port'
        ,'=': 'Socket'
        ,'F': 'Regular file'
    }
    CHANGE_TYPES={
        '-': 'The path has been removed'
       ,'+': 'The path has been created'
       ,'M': 'The path has been modified'
       ,'R': 'The path has been renamed'
    }
    def __init__(self, row, snap_left, snap_right):
        self.no_from_snap=False
        self.to_present=False
        if isinstance(snap_left, str) and snap_left == '(na-first)':
            self.no_from_snap=True
            snap_left = None
        elif not isinstance(snap_left, Snapshot):
            raise AssertionError(f"snap_left must be either a Snapshot or str('na-first'). Got: {type(snap_left)}")

        if isinstance(snap_right, str) and snap_right == '(present)':
            self.to_present=True
            snap_right = None

        elif not isinstance(snap_right, Snapshot):
            raise AssertionError(f"snap_left must be either a Snapshot. Got: {type(snap_right)}")

        if not self.no_from_snap and not self.to_present and snap_left.creation > snap_right.creation:
            raise AssertionError(f"diff from creation ({snap_left.creation}) is > to diff_to creation ({snap_right.creation})")

        self.snap_left = snap_left
        self.snap_right = snap_right

        if len(row) == 4:
            (inode_ts, chg_type, file_type, path) = row
            path_new = None
        elif len(row) == 5:
            (inode_ts, chg_type, file_type, path, path_new) = row
        else:
            raise Exception(f"Unexpected len: {len(row)}. Row = {row}")

        chg_time = datetime.fromtimestamp(int(inode_ts[:inode_ts.find('.')]))
        self.chg_ts = inode_ts
        self.chg_time = chg_time
        self.chg_type = chg_type
        self.file_type = file_type
        if file_type == '/':
            self.file = None
            self.path = path
            self.path_full = path
        else:
            (f, p) = splitPath(path)
            self.file = f
            self.path = p
            self.path_full = path

        if file_type == '/':
            self.file_new = None
            self.path_new = path_new
            self.path_full_new = path_new
        else:
            (f_new, p_new) = splitPath(path_new) if not path_new is None else (None, None)
            self.file_new = f_new
            self.path_new = p_new
            self.path_full_new = path_new

    file_type_full = property(lambda self: Diff.get_file_type(self.file_type))
    chg_type_full = property(lambda self: Diff.get_change_type(self.chg_type))

    # Ressolves path to resource on left side of diff in zfs_snapshot dir
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
        path_full = self.path_full_new if self.chg_type == 'R' else self.path_full
        return "{}{}".format(snap_path, path_full.replace(self.snap_left.dataset.mountpoint, ''))
    snap_path_right = property(_get_snap_path_right)
    
    @staticmethod
    def get_file_type(s):
        assert (isinstance(s, str) and not s == ''), "argument must be a non-empty string"
        assert s in Diff.FILE_TYPES, f"ZFS Diff File type is invalid: '{s}'"
        return Diff.FILE_TYPES[s]

    @staticmethod
    def get_change_type(s):
        assert (isinstance(s, str) and not s == ''), "argument must be a non-empty string"
        assert s in Diff.CHANGE_TYPES, f"ZFS Diff Change type is invalid: '{s}'"
        return Diff.CHANGE_TYPES[s]

    def __str__(self):
        return "<Diff> {0} [{1}][{2}] {3}{4}".format(
            self.chg_time.strftime("%Y-%m-%d %H:%M:%S")
            ,self.chg_type, self.file_type
            ,self.path_full, ('' if not self.path_full_new else ' --> '+self.path_new))
    __repr__ = __str__

''' END ZFS Entities '''

ZFS_INT_PROPS =  set("allocated,available,capacity,checkpoint,createtxg,expandsize,filesystem_count,filesystem_limit,fragmentation,free,freeing,leaked,logicalreferenced,logicalused,objsetid,quota,referenced,refquota,refreservation,reservation,size,snapshot_count,snapshot_limit,used,usedbychildren,usedbydataset,usedbyrefreservation,usedbysnapshots,userrefs,volsize,written".split(','))

''' General Utilities '''

# buildTimedelta()
# Builds timedelta from string:
# . tdelta is a timedelta -or- str(nC) where: n is an integer > 0 and C is one of:
#   . Y=year, M=month, W=week, D=day, h=hour, m=minute, s=second
# Note: month and year are imprecise and assume 30.4 and 365 days
def buildTimedelta(tdelta) -> timedelta:
    if isinstance(tdelta, timedelta): return tdelta
    
    if not isinstance(tdelta, str):
        raise KeyError('tdelta must be a string')
    elif len(tdelta) < 2:
        raise KeyError('len(tdelta) must be >= 2')
    n = tdelta[:-1]
    try:
        n = int(n)
        if n < 1: raise KeyError('tdelta must be > 0')
    except ValueError as ex:
        raise KeyError(f'Value passed for tdelta does not contain a number: {tdelta}')
    
    c = tdelta[-1:]
    if c == 'h':
        return timedelta(hours=n)
    elif c == 'm':
        return timedelta(minutes=n)
    elif c == 's':
        return timedelta(seconds=n)
    elif c == 'D':
        return timedelta(days=n)
    elif c == 'W':
        return timedelta(weeks=n)
    elif c == 'M':
        return timedelta(days=n*(365/12))
    elif c == 'Y':
        return timedelta(days=n*365)
    else:
        raise KeyError('Unexpected datetime identifier, expecting one of Y,M,W,D,h,m,s')


# calcDateRange()
# Calculates a date range based on tdelta string passed
# tdelta is a timedelta -or- str(nC) where: n is an integer > 0 and C is one of:
#  . Y=year, M=month, W=week, D=day, h=hour, m=minute, s=second
# If dt_from is defined, return tuple: (dt_from, dt_from+tdelta)
# If dt_to is defined, return tuple: (dt_from-tdelta, dt_to)
def calcDateRange(tdelta, dt_from:datetime=None, dt_to:datetime=None) -> tuple:
    if tdelta is None: raise KeyError('tdelta is required')
    if dt_from and dt_to:
        raise KeyError('Only one of dt_from or dt_to must be defined')
    elif (not dt_from and not dt_to):
        raise KeyError('Please specify one of dt_from or dt_to')
    elif dt_from and not isinstance(dt_from, datetime):
        raise KeyError('dt_from must be  a datetime')
    elif dt_to and not isinstance(dt_to, datetime):
        raise KeyError('dt_to must be  a datetime')

    td = buildTimedelta(tdelta)
    
    if dt_from:
        return (dt_from, (dt_from + td))
    else:
        return ((dt_to - td), dt_to)


def splitPath(s):
    assert isinstance(s, str), f"String not passed. Got: {type(s)}"
    s = s.strip()
    assert not s == '', "Empty string passed"
    f = os.path.basename(s)
    if len(f) == 0: return ('', s[:-1] if s[-1:] == '/' else s)
    p = s[:-(len(f))-1]
    return f, p


def simplify(x):
    '''Take a list of tuples where each tuple is in form [v1,v2,...vn]
    and then coalesce all tuples tx and ty where tx[v1] equals ty[v2],
    preserving v3...vn of tx and discarding v3...vn of ty.
    m = [
    (1,2,"one"),
    (2,3,"two"),
    (3,4,"three"),
    (8,9,"three"),
    (4,5,"four"),
    (6,8,"blah"),
    ]
    simplify(x) -> [[1, 5, 'one'], [6, 9, 'blah']]
    '''
    y = list(x)
    if len(x) < 2: return y
    for idx,o in enumerate(list(y)):
        for idx2,p in enumerate(list(y)):
            if idx == idx2: continue
            if o and p and o[0] == p[1]:
                y[idx] = None
                y[idx2] = list(p)
                y[idx2][0] = p[0]
                y[idx2][1] = o[1]
    return [ n for n in y if n is not None ]

def uniq(seq, idfun=None):
    '''Makes a sequence 'unique' in the style of UNIX command uniq'''
    # order preserving
    if idfun is None:
        def idfun(x): return x
    seen = {}
    result = []
    for item in seq:
        marker = idfun(item)
        # in old Python versions:
        # if seen.has_key(marker)
        # but in new ones:
        if marker in seen: continue
        seen[marker] = 1
        result.append(item)
    return result

def expand_user(path):
    return pathlib.Path(path).expanduser()
        

# Ignore snapshots with exact same timestamp
# . Edge cases that can happen and muck up stuff
# . Handles list(of Snapshot) and list(of tuple(of idx, Snapshot))
def removeDuplicateSnapshotsByDate(snapshots):
    _ret = []
    for i, snap_rec in enumerate(snapshots):
        if isinstance(snap_rec, Snapshot):
            snap = snap_rec
        elif isinstance(snap_rec, tuple):
            (idx, snap) = snap_rec
        else:
            raise Exception(f"Invalid snapshot list passed. Got {type(snap_rec)} at record {i}")

        if i > 0 and snap.creation == snap_last.creation: continue

        _ret.append(snap_rec)

        snap_last = snap
    return _ret


''' END Utilities '''



''' LEGACY DUCK PUNCHING'''

# Work-around for check_output not existing on Python 2.6, as per
# http://stackoverflow.com/questions/4814970/subprocess-check-output-doesnt-seem-to-exist-python-2-6-5
# The implementation is lifted from
# http://hg.python.org/cpython/file/d37f963394aa/Lib/subprocess.py#l544
if "check_output" not in dir( subprocess ): # duck punch it in!
    def f(*popenargs, **kwargs):
        if 'stdout' in kwargs:
            raise ValueError('stdout argument not allowed, it will be overridden.')
        process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
        output, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            cmd = kwargs.get("args")
            if cmd is None:
                cmd = popenargs[0]
            raise subprocess.CalledProcessError(retcode, cmd) # , output=output)
        return output
    subprocess.check_output = f


''' END LEGACY DUCK PUNCHING '''

# No operation lambda dropin or breakpoint marker
def noop(*args, **kwargs):
    if len(args): return args[0]
