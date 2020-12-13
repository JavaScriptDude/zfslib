#########################################
# .: zfslib.py :.
# Libraries for reading data from zfs with Python
# This library is a collation of https://github.com/Rudd-O/zfs-tools and customizations into a single library
# Notes: 
# . Code from Rudd-O/zfs-tools was take on 2020/12/12 16:00 EST
# . Many zfs-tools methods have been removed 
# .: Usage :.
# see test.py source code
# .: Other :.
# Author: Timothy C. Quinn
# Home: https://github.com/JavaScriptDude/zfslib
# Licence: https://opensource.org/licenses/MIT
#########################################


import subprocess, os, itertools, warnings, sys, fnmatch
from queue import Queue
from threading import Thread
from collections import OrderedDict
from datetime import datetime, time, timedelta



''' 
    From (zfstools::connection.py) 
'''

'''
ZFS connection classes
'''
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


class ZFSConnection:
    host = None
    _poolset = None
    _dirty = True
    _trust = False
    _properties = None
    def __init__(self,host="localhost", trust=False, sshcipher=None, properties=None, identityfile=None, knownhostsfile=None, verbose=False):
        self.host = host
        self._trust = trust
        self._properties = properties if properties else []
        self._poolset= PoolSet(self)
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

    def get_poolset(self):
        if self._dirty:
            properties = [ 'creation' ] + self._properties
            stdout2 = subprocess.check_output(self.command + ["zfs", "list", "-Hpr", "-o", ",".join( ['name'] + properties ), "-t", "all"])

            cmd = self.command + ["zfs", "list", "-Hpr"]
            # print("cmd = {}".format(' '.join(cmd)))
            stdout_def = subprocess.check_output(cmd)

            self._poolset.parse_zfs_r_output(stdout2,stdout_def,properties)
            self._dirty = False
        return self._poolset


    def snapshot_recursively(self,name,snapshotname,properties={}):
        plist = sum( map( lambda x: ['-o', '%s=%s' % x ], properties.items() ), [] )
        subprocess.check_call(self.command + ["zfs", "snapshot", "-r" ] + plist + [ "%s@%s" % (name, snapshotname)])
        self._dirty = True


# END tools::connection.py


''' (from zfs-tools::models.py)
Tree models for the ZFS tools
'''
# ZFSItem is an 'abstract' class for Pool, Dataset and Snapshot
class ZFSItem(object):
    name = None
    children = None
    _properties = None
    parent = None
    invalidated = False

    def __init__(self, name, parent=None):
        self.name = name
        self.children = []
        self._properties = {}
        if parent:
            self.parent = parent
            self.parent.add_child(self)

    def add_child(self, child):
        self.children.append(child)
        return child

    def get_child(self, name):
        child = [ c for c in self.children if c.name == name and isinstance(c, Dataset) ]
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

    def get_path(self):
        if not self.parent: return self.name
        return "%s/%s" % (self.parent.get_path(), self.name)

    def get_pool(self):
        p=self
        while True:
            if isinstance(p, Pool): return p
            p = p.parent
            
    def get_connection(self):
        p = self.get_pool()
        return p.connection

    def get_relative_name(self):
        if not self.parent: return self.name
        return self.get_path()[len(self.parent.get_path()) + 1:]

    def walk(self):
        assert not self.invalidated, "%s invalidated" % self
        yield self
        for c in self.children:
            for element in c.walk():
                yield element

    def __iter__(self):
        return self.walk()

    def get_property(self,name):
        return self._properties[ name ]
    
    def get_creation(self):
        return datetime.fromtimestamp(int(self._properties["creation"]))

    # returns list(of tuple(of depth, Dataset))
    def get_all_datasets(self, depth:int=0):
        a = []
        for c in self.children:
            if isinstance(c, Dataset):
                a.append( (depth, c) )
                a = a + c.get_all_datasets(depth+1)
        return a



class Dataset(ZFSItem):

    def get_snapshots(self, flt=True):
        if flt is True: flt = lambda _:True
        children = [ c for c in self.children if isinstance(c, Snapshot) and flt(c) ]
        return children

    def get_snapshot(self, name):
        children = [ c for c in self.get_snapshots() if c.name == name ]
        assert len(children) < 2
        if not children: raise KeyError(name)
        return children[0]


    # find_snapshots(dict) - Query all snapshots in Dataset and optionally filter by: 
    #  * name: Snapshot name (wildcard supported) 
    #  * dt_from: datetime to start
    #  * tdelta: timedelta -or- string of nC where: n is an integer > 0 and C is one of y,m,d,H,M,S. Eg 5H = 5 Hours
    #  * dt_to: datetime to stop 
    #  * Date searching is any combination of:
    #    .  (dt_from --> dt_to) | (dt_from --> dt_from + tdelta) | (dt_to - tdelta --> dt_to)
    def find_snapshots(self, find_opts:dict) -> list:

        def __assert(k, types):
            if k == 'find_opts':
                v=find_opts
            else:
                if not k in find_opts: return None
                v = find_opts[k]
            bOk=False
            for t in types:
                if isinstance(v, t): bOk=True
            assert bOk, 'Invalid type for param {}. Expecting {} but got: {}'.format(k, types, type(v))
            return v
        find_opts = __assert('find_opts', [dict])
        name = __assert('name', [str])
        dt_from = __assert('dt_from', [datetime])
        dt_to = __assert('dt_to', [datetime])
        tdelta = __assert('tdelta', [str, timedelta])
        

        def __fil_n(snap):
            if not name is None and not fnmatch.fnmatch(snap.name, name): return False
            return True

        def __fil_dt(snap):
            if not __fil_n(snap): return False
            cdate = snap.get_creation()
            if cdate < dt_f: return False
            if cdate > dt_t: return False
            return True

        if not dt_from and not dt_to and not tdelta:
            f = __fil_n

        else:
            f=__fil_dt
            if dt_from and dt_to and not tdelta:
                dt_f = dt_from
                dt_t = dt_to
            else:
                (dt_f, dt_t) = calcDateRange(tdelta=tdelta, dt_from=dt_from, dt_to=dt_to)
        
        return self.get_snapshots(flt=f)


    # get_diffs() - Gets Diffs in snapshot or between snapshots (if snap_to is specified)
    # ignore - list of glob expressions to ignore (eg ['*_pycache_*'])
    # file_type - Filter on the following
        # B       Block device
        # C       Character device
        # /       Directory
        # >       Door
        # |       Named pipe
        # @       Symbolic link
        # P       Event port
        # =       Socket
        # F       Regular file
    # chg_type - Filter on the following:
        # -       The path has been removed
        # +       The path has been created
        # M       The path has been modified
        # R       The path has been renamed
    def get_diffs(self, snap_from, snap_to=None, ignore:list=None, file_type:str=None, chg_type:str=None) -> list:
        if snap_from is None or not isinstance(snap_from, Snapshot):
            raise Exception("snap_from must be a Snapshot")
        if not snap_to is None and not isinstance(snap_to, Snapshot):
            raise Exception("snap_to must be a Snapshot")
        if not ignore is None and not isinstance(ignore, list):
            raise Exception("snap_to must be a Snapshot")

        cmd = self.get_connection().command + ["zfs", "diff", "-FHt", snap_from.get_path()]
        if snap_to: cmd = cmd + [snap_to.get_path()]

        stdout = subprocess.check_output(cmd)
        def __row(s):
            s = s.decode('utf-8') if isinstance(s, bytes) else s
            return s.strip().split( '\t' )
        rows = list(map(lambda s: __row(s), stdout.splitlines()))
        diffs = []
        for row in rows:
            d = Diff(row)
            if not file_type is None and not d.file_type == file_type: continue
            if not chg_type is None and not d.chg_type == chg_type: continue
            if not ignore is None:
                bIgn = False
                for ign in ignore:
                    if fnmatch.fnmatch(d.path_full, ign) \
                        or (not d.path_r is None and fnmatch.fnmatch(d.path_r_full, ign)):
                        bIgn = True
                        break
                if bIgn: continue
            diffs.append(d)

        return diffs


    def lookup(self, name):  # FINISH THIS
        if "@" in name:
            path, snapshot = name.split("@")
        else:
            path = name
            snapshot = None

        if "/" not in path:
            try: dset = self.get_child(path)
            except KeyError: raise KeyError("No such dataset %s at %s" % (path, self.get_path()))
            if snapshot:
                try: dset = dset.get_snapshot(snapshot)
                except KeyError: raise KeyError("No such snapshot %s at %s" % (snapshot, dset.get_path()))
        else:
            head, tail = path.split("/", 1)
            try: child = self.get_child(head)
            except KeyError: raise KeyError("No such dataset %s at %s" % (head, self.get_path()))
            if snapshot: tail = tail + "@" + snapshot
            dset = child.lookup(tail)

        return dset

    def __str__(self):
        return "<Dataset:  %s>" % self.get_path()
    __repr__ = __str__



class Pool(ZFSItem):
    def __init__(self, name, conn:ZFSConnection):
        self.name = name
        self.children = []
        self._properties = {}
        self.connection = conn
        
    def __str__(self):
        return "<Pool:     %s>" % self.get_path()
    __repr__ = __str__



class Snapshot(ZFSItem):
    # def __init__(self,name):
        # Dataset.__init__(self,name)
    def get_path(self):
        if not self.parent: return self.name
        return "%s@%s" % (self.parent.get_path(), self.name)
    
    def __str__(self):
        return "<Snapshot: %s>" % self.get_path()
    __repr__ = __str__



class PoolSet(object):  # maybe rewrite this as a dataset or something?
    pools = None

    def __init__(self, conn:ZFSConnection):
        self.connection=conn
        self.pools = {}

    def lookup(self, name):
        if "@" in name:
            path, snapshot = name.split("@")
        else:
            path = name
            snapshot = None

        if "/" not in path:
            try: dset = self.pools[path]
            except KeyError: raise KeyError("No such pool %s" % (name))
            if snapshot:
                try: dset = dset.get_snapshot(snapshot)
                except KeyError: raise KeyError("No such snapshot %s at %s" % (snapshot, dset.get_path()))
        else:
            head, tail = path.split("/", 1)
            try: pool = self.pools[head]
            except KeyError: raise KeyError("No such pool %s" % (head))
            if snapshot: tail = tail + "@" + snapshot
            dset = pool.lookup(tail)

        return dset

    def parse_zfs_r_output(self, zfs_r_output, zfs_def_output, properties = None):
        """Parse the output of tab-separated zfs list.

        properties must be a list of property names expected to be found as
        tab-separated entries on each line of zfs_r_output after the
        dataset name and a tab.
        E.g. if properties passed here was ['creation'], we would expect
        each zfs_r_output line to look like 'dataset	3249872348'
        """


        #Parse std output
        def __row(s):
            s = s.decode('utf-8') if isinstance(s, bytes) else s
            return s.strip().split( '\t' )
        std_rows = list(map(lambda s: __row(s), zfs_def_output.splitlines()))
        std_data={}
        for std_row in std_rows:
            if len(std_row) == 5:
                (std_name, std_used, std_avail, std_ref, std_mount) = std_row
            else:
                raise Exception("Unexpected length returned by command: 'zfs list -Hpr'. Row returned: {}".format(' '.join(cmd), std_row))
            std_data[std_name] = (std_used, std_avail, std_ref, std_mount)

        try:
            properties = ['name', 'creation'] if properties == None else ['name'] + properties
        except TypeError:
            assert 0, repr(properties)

        def extract_properties(s):
            s = s.decode('utf-8') if isinstance(s, bytes) else s
            items = s.strip().split( '\t' )
            assert len( items ) == len( properties ), (properties, items)
            propvalues = map( lambda x: None if x == '-' else x, items[ 1: ] )
            return [ items[ 0 ], zip( properties[ 1: ], propvalues ) ]

        # make into array
        creations = OrderedDict([ extract_properties( s ) for s in zfs_r_output.splitlines() if s.strip() ])

        # names of pools
        old_dsets = [ x.get_path() for x in self.walk() ]
        old_dsets.reverse()
        new_dsets = creations.keys()

        for dset in new_dsets:
            if "@" in dset:
                dset, snapshot = dset.split("@")
            else:
                snapshot = None
            if "/" not in dset:  # pool name
                if dset not in self.pools:
                    self.pools[dset] = Pool(dset, self.connection)
                    fs = self.pools[dset]
            poolname, pathcomponents = dset.split("/")[0], dset.split("/")[1:]
            fs = self.pools[poolname]
            for pcomp in pathcomponents:
                # traverse the child hierarchy or create if that fails
                try: fs = fs.get_child(pcomp)
                except KeyError:
                    fs = Dataset(pcomp, fs)

            if snapshot:
                if snapshot not in [ x.name for x in fs.children ]:
                    fs = Snapshot(snapshot, fs)

            fs._properties.update( creations[fs.get_path()] )
            
            (std_used, std_avail, std_ref, std_mount) = std_data[dset]
            # std_avail is avail, std_ref is usedds
            fs._properties['mountpoint'] = std_mount
            fs._properties['used'] = std_used


        for dset in old_dsets:
            if dset not in new_dsets:
                if "/" not in dset and "@" not in dset:  # a pool
                    self.remove(dset)
                else:
                    d = self.lookup(dset)
                    d.parent.remove(d)


    


    def remove(self, name):  # takes a NAME, unlike the child that is taken in the remove of the dataset method
        for c in self.pools[name].children:
            self.pools[name].remove(c)
        self.pools[name].invalidated = True
        del self.pools[name]



    def __getitem__(self, name):
        return self.pools[name]

    def __str__(self):
        return "<PoolSet at %s>" % id(self)
    __repr__ = __str__

    def walk(self):
        for item in self.pools.values():
            for dset in item.walk():
                yield dset

    def __iter__(self):
        return self.walk()
# END tools::models.py



''' (from zfs-tools::util.py)
Miscellaneous utility functions
'''

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


class SpecialPopen(subprocess.Popen):
    def __init__(self, *a, **kw):
        self._saved_args = a[0] if kw.get("args") is None else kw.get("args")
        subprocess.Popen.__init__(self, *a, **kw)


def progressbar(pipe, bufsize=-1, ratelimit=-1):

    def clpbar(cmdname):
        barargs = []
        if bufsize != -1:
            barargs = ["-bs", str(bufsize)]
        if ratelimit != -1:
            barargs = barargs + ['-th', str(ratelimit)]
        barprg = SpecialPopen(
            [cmdname, "-dan"] + barargs,
            stdin=pipe, stdout=subprocess.PIPE, bufsize=bufsize)
        return barprg

    def pv(cmdname):
        barargs = []
        if bufsize != -1:
            barargs = ["-B", str(bufsize)]
        if ratelimit != -1:
            barargs = barargs + ['-L', str(ratelimit)]
        barprg = SpecialPopen(
            [cmdname, "-ptrb"] + barargs,
            stdin=pipe, stdout=subprocess.PIPE, bufsize=bufsize)
        return barprg

    barprograms = [
        ("bar", clpbar),
        ("clpbar", clpbar),
        ("pv", pv),
    ]

    for name, func in barprograms:
        try:
            subprocess.call([name, '-h'], stdout=open(os.devnull, "w"), stderr=open(os.devnull, "w"), stdin=open(os.devnull, "r"))
        except OSError as e:
            if e.errno == 2: continue
            assert 0, "not reached while searching for clpbar or pv"
        return func(name)
    raise OSError(2, "no such file or directory searching for clpbar or pv")

def stderr(text):
    """print out something to standard error, followed by an ENTER"""
    sys.stderr.write(text)
    sys.stderr.write("\n")

__verbose = False
def verbose_stderr(*args, **kwargs):
    global __verbose
    if __verbose: stderr(*args, **kwargs)

def set_verbose(boolean):
    global __verbose
    __verbose = boolean
# END tools::util.py



# (qcorelite.py - code from my own internal utility)

# Calculates a date range based on tdelta string passed
# tdelta is a timedelta -or- str(nC) where: n is an integer > 0 and C is one of:
# . y=year, m=month, d=day, H=hour, M=minute, s=second
# If dt_from is defined, return tuple: (dt_from, dt_from+tdelta)
# If dt_to is defined, return tuple: (dt_from-tdelta, dt_to)
def calcDateRange(tdelta, dt_from:datetime=None, dt_to:datetime=None) -> tuple:
    if dt_from and dt_to:
        raise Exception('Only one of dt_from or dt_to must be defined')
    elif (not dt_from and not dt_to):
        raise Exception('Please specify one of dt_from or dt_to')
    elif tdelta is None:
        raise Exception('tdelta is required')
    elif dt_from and not isinstance(dt_from, datetime):
        raise Exception('dt_from must be  a datetime')
    elif dt_to and not isinstance(dt_to, datetime):
        raise Exception('dt_to must be  a datetime')


    if isinstance(tdelta, timedelta):
        td = tdelta

    else:
        if not isinstance(tdelta, str):
            raise Exception('tdelta must be a string or timedelta')
        elif len(tdelta) < 2:
            raise Exception('len(tdelta) must be >= 2')
        n = tdelta[:-1]
        try:
            n = int(n)
            if n < 1: raise Exception('tdelta must be > 0')
        except ValueError as ex:
            raise Exception('Value passed for tdelta does not contain a number: {}'.format(tdelta))
        
        c = tdelta[-1:]
        if c == 'H':
            td = timedelta(hours=n)
        elif c == 'M':
            td = timedelta(minutes=n)
        elif c == 'S':
            td = timedelta(seconds=n)
        elif c == 'd':
            td = timedelta(days=n)
        elif c == 'm':
            td = timedelta(months=n)
        elif c == 'y':
            td = timedelta(years=n)
        else:
            raise Exception('Unexpected datetime identifier, expecting one of y,m,d,H,M,S.')
    
    if dt_from:
        return (dt_from, (dt_from + td))
    else:
        return ((dt_to - td), dt_to)


def splitPath(s):
    f = os.path.basename(s)
    p = s[:-(len(f))-1]
    return f, p

# END qcorelite.py



class Diff():
    def __init__(self, row:list, snap:Snapshot=None):
        
        if len(row) == 4:
            (inode_ts, chg_type, file_type, path_l) = row
            path_r = None
        elif len(row) == 5:
            (inode_ts, chg_type, file_type, path_l, path_r) = row
        else:
            raise Exeption("Unexpected len: {}. Row = {}".format(len(row), row))

        chg_time = datetime.fromtimestamp(int(inode_ts[:inode_ts.find('.')]))
        self.snapshot=snap
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

    def __str__(self):
        return "<Diff> {0} [{1}][{2}] {3}{4}".format(
             self.chg_time.strftime("%Y-%m-%d %H:%M:%S")
            ,self.chg_type, self.file_type
            ,self.path_full, ('' if not self.path_r_full else ' --> '+self.path_r))
    __repr__ = __str__












