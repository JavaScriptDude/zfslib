#########################################
# .: zfslib.py :.
# Libraries for reading data from zfs with Python
# This library is a collation of https://github.com/Rudd-O/zfs-tools and customizations into a single library
# Note: code from Rudd-O/zfs-tools was take on 2020/12/12 16:00 EST
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
        self._poolset= PoolSet()
        self.verbose = verbose
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

    def _get_poolset(self):
        if self._dirty:
            properties = [ 'creation' ] + self._properties
            stdout2 = subprocess.check_output(self.command + ["zfs", "list", "-Hpr", "-o", ",".join( ['name'] + properties ), "-t", "all"])

            cmd = self.command + ["zfs", "list", "-Hpr"]
            # print("cmd = {}".format(' '.join(cmd)))
            stdout_def = subprocess.check_output(cmd)

            self._poolset.parse_zfs_r_output(stdout2,stdout_def,properties)
            self._dirty = False
        return self._poolset
    pools = property(_get_poolset)

    def create_dataset(self,name):
        subprocess.check_call(self.command + ["zfs", "create", name])
        self._dirty = True
        return self.pools.lookup(name)

    def destroy_dataset(self, name):
        subprocess.check_call(self.command + ["zfs", "destroy", name])
        self._dirty = True

    def destroy_recursively(self, name, returnok=False):
        """If returnok, then simply return success as a boolean."""
        ok = True
        cmd = self.command + ["zfs", "destroy", '-r', name]
        if returnok:
            ok = subprocess.call(cmd) == 0
        else:
            subprocess.check_call(cmd)
        self._dirty = True
        return ok

    def snapshot_recursively(self,name,snapshotname,properties={}):
        plist = sum( map( lambda x: ['-o', '%s=%s' % x ], properties.items() ), [] )
        subprocess.check_call(self.command + ["zfs", "snapshot", "-r" ] + plist + [ "%s@%s" % (name, snapshotname)])
        self._dirty = True

    def send(self,name,opts=None,bufsize=-1,compression=False,lockdataset=None):
        if not opts: opts = []
        cmd = list(self.command)
        if compression and cmd[0] == 'ssh': cmd.insert(1,"-C")
        if lockdataset is not None:
            cmd += ["zflock"]
            if self.verbose:
                cmd += ["-v"]
            cmd += [lockdataset, "--"]
        cmd += ["zfs", "send"] + opts + [name]
        p = SpecialPopen(cmd,stdin=file(os.devnull),stdout=subprocess.PIPE,bufsize=bufsize)
        return p

    def receive(self,name,pipe,opts=None,bufsize=-1,compression=False,lockdataset=None):
        if not opts: opts = []
        cmd = list(self.command)
        if compression and cmd[0] == 'ssh': cmd.insert(1,"-C")
        if lockdataset is not None:
            cmd += ["zflock"]
            if self.verbose:
                cmd += ["-v"]
            cmd += [lockdataset, "--"]
        cmd = cmd + ["zfs", "receive"] + opts + [name]
        p = SpecialPopen(cmd,stdin=pipe,bufsize=bufsize)
        return p

    def transfer(self, dst_conn, s, d, fromsnapshot=None, showprogress=False, bufsize=-1, send_opts=None, receive_opts=None, ratelimit=-1, compression=False, locksrcdataset=None, lockdstdataset=None):
        if send_opts is None: send_opts = []
        if receive_opts is None: receive_opts = []

        queue_of_killables = Queue()

        if fromsnapshot: fromsnapshot=["-i",fromsnapshot]
        else: fromsnapshot = []
        sndprg = self.send(s, opts=[] + fromsnapshot + send_opts, bufsize=bufsize, compression=compression, lockdataset=locksrcdataset)
        sndprg_supervisor = Thread(target=lambda: queue_of_killables.put((sndprg, sndprg.wait())))
        sndprg_supervisor.start()

        if showprogress:
            try:
                        barprg = progressbar(pipe=sndprg.stdout,bufsize=bufsize,ratelimit=ratelimit)
                        barprg_supervisor = Thread(target=lambda: queue_of_killables.put((barprg, barprg.wait())))
                        barprg_supervisor.start()
                        sndprg.stdout.close()
            except OSError:
                        os.kill(sndprg.pid,15)
                        raise
        else:
            barprg = sndprg

        try:
                        rcvprg = dst_conn.receive(d,pipe=barprg.stdout,opts=["-Fu"]+receive_opts,bufsize=bufsize,compression=compression, lockdataset=lockdstdataset)
                        rcvprg_supervisor = Thread(target=lambda: queue_of_killables.put((rcvprg, rcvprg.wait())))
                        rcvprg_supervisor.start()
                        barprg.stdout.close()
        except OSError:
                os.kill(sndprg.pid, 15)
                if sndprg.pid != barprg.pid: os.kill(barprg.pid, 15)
                raise

        dst_conn._dirty = True
        allprocesses = set([rcvprg, sndprg]) | ( set([barprg]) if showprogress else set() )
        while allprocesses:
            diedprocess, retcode = queue_of_killables.get()
            allprocesses = allprocesses - set([diedprocess])
            if retcode != 0:
                [ p.kill() for p in allprocesses ]
                raise subprocess.CalledProcessError(retcode, diedprocess._saved_args)
# END tools::connection.py


''' (from zfs-tools::models.py)
Tree models for the ZFS tools
'''
# ZfsItem is an 'abstract' class
class ZfsItem(object):
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



class Dataset(ZfsItem):

    def get_snapshots(self, flt=True):
        if flt is True: flt = lambda _:True
        children = [ c for c in self.children if isinstance(c, Snapshot) and flt(c) ]
        return children

    def get_snapshot(self, name):
        children = [ c for c in self.get_snapshots() if c.name == name ]
        assert len(children) < 2
        if not children: raise KeyError(name)
        return children[0]

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



class Pool(ZfsItem):
    def __str__(self):
        return "<Pool:     %s>" % self.get_path()
    __repr__ = __str__



class Snapshot(ZfsItem):
    # def __init__(self,name):
        # Dataset.__init__(self,name)
    def get_path(self):
        if not self.parent: return self.name
        return "%s@%s" % (self.parent.get_path(), self.name)
    
    def __str__(self):
        return "<Snapshot: %s>" % self.get_path()
    __repr__ = __str__


class PoolSet:  # maybe rewrite this as a dataset or something?
    pools = None

    def __init__(self):
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
                    self.pools[dset] = Pool(dset)
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



''' (from zfs-tools::sync.py)
Synchronization-related functionality
'''


# it is time to determine which datasets need to be synced
# we walk the entire dataset structure, and sync snapshots recursively
def recursive_replicate(s, d):
    sched = []

    # we first collect all snapshot names, to later see if they are on both sides, one side, or what
    all_snapshots = []
    if s: all_snapshots.extend(s.get_snapshots())
    if d: all_snapshots.extend(d.get_snapshots())
    all_snapshots = [ y[1] for y in sorted([ (x.get_property('creation'), x.name) for x in all_snapshots ]) ]
    snapshot_pairs = []
    for snap in all_snapshots:
        try: ssnap = s.get_snapshot(snap)
        except (KeyError, AttributeError): ssnap = None
        try: dsnap = d.get_snapshot(snap)
        except (KeyError, AttributeError): dsnap = None
        # if the source snapshot exists and is not already in the table of snapshots
        # then pair it up with its destination snapshot (if it exists) or None
        # and add it to the table of snapshots
        if ssnap and not snap in [ x[0].name for x in snapshot_pairs ]:
            snapshot_pairs.append((ssnap, dsnap))

    # now we have a list of all snapshots, paired up by name, and in chronological order
    # (it's quadratic complexity, but who cares)
    # now we need to find the snapshot pair that happens to be the the most recent common pair
    found_common_pair = False
    for idx, (m, n) in enumerate(snapshot_pairs):
        if m and n and m.name == n.name:
            found_common_pair = idx

    # we have combed through the snapshot pairs
    # time to check what the latest common pair is
    if not s.get_snapshots():
        if d is None:
            # well, no snapshots in source, just create a stub in the target
            sched.append(("create_stub", s, d, None, None))
    elif found_common_pair is False:
        # no snapshot is in common, problem!
        # theoretically destroying destination dataset and resyncing it recursively would work
        # but this requires work in the optimizer that comes later
        if d is not None and d.get_snapshots():
            warnings.warn("Asked to replicate %s into %s but %s has snapshots and both have no snapshots in common!" % (s, d, d))
        # see source snapshots
        full_source_snapshots = [ y[1] for y in sorted([ (x.get_property('creation'), x) for x in s.get_snapshots() ]) ]
        # send first snapshot as full snapshot
        sched.append(("full", s, d, None, full_source_snapshots[0]))
        if len(full_source_snapshots) > 1:
            # send other snapshots as incremental snapshots
            sched.append(("incremental", s, d, full_source_snapshots[0], full_source_snapshots[-1]))
    elif found_common_pair == len(snapshot_pairs) - 1:
        # the latest snapshot of both datasets that is common to both, is the latest snapshot in the source
        # we have nothing to do here because the datasets are "in sync"
        pass
    else:
        # the source dataset has more recent snapshots, not present in the destination dataset
        # we need to transfer those
        snapshots_to_transfer = [ x[0] for x in snapshot_pairs[found_common_pair:] ]
        for n, x in enumerate(snapshots_to_transfer):
            if n == 0: continue
            sched.append(("incremental", s, d, snapshots_to_transfer[n - 1], x))

    # now let's apply the same argument to the children
    children_sched = []
    for c in [ x for x in s.children if not isinstance(x, Snapshot) ]:
        try: cd = d.get_child(c.name)
        except (KeyError, AttributeError): cd = None
        children_sched.extend(recursive_replicate(c, cd))

    # and return our schedule of operations to the parent
    return sched + children_sched

def optimize_coalesce(operation_schedule):
    # now let's optimize the operation schedule
    # this optimization is quite basic
    # step 1: coalesce contiguous operations on the same file system

    operations_grouped_by_source = itertools.groupby(
        operation_schedule,
        lambda op: op[1]
    )
    new = []
    for _, opgroup in [ (x, list(y)) for x, y in operations_grouped_by_source ]:
        if not opgroup:  # empty opgroup
            continue
        if opgroup[0][0] == 'full':  # full operations
            new.extend(opgroup)
        elif opgroup[0][0] == 'create_stub':  # create stub operations
            new.extend(opgroup)
        elif opgroup[0][0] == 'incremental':  # incremental
            # 1->2->3->4 => 1->4
            new_ops = [ (srcs, dsts) for _, _, _, srcs, dsts in opgroup ]
            new_ops = simplify(new_ops)
            for srcs, dsts in new_ops:
                new.append(tuple(opgroup[0][:3] + (srcs, dsts)))
        else:
            assert 0, "not reached: unknown operation type in %s" % opgroup
    return new

def optimize_recursivize(operation_schedule):
    def recurse(dataset, func):
        results = []
        results.append((dataset, func(dataset)))
        results.extend([ x for child in dataset.children if child.__class__ != Snapshot for x in recurse(child, func) ])
        return results

    def zero_out_sched(dataset):
        dataset._ops_schedule = []

    def evict_sched(dataset):
        dataset._ops_schedule = []

    operations_grouped_by_source = itertools.groupby(
        operation_schedule,
        lambda op: op[1]
    )
    operations_grouped_by_source = [ (x, list(y)) for x, y in operations_grouped_by_source ]

    roots = set()
    for root, opgroup in operations_grouped_by_source:
        while root.parent is not None:
            root = root.parent
        roots.add(root)

    for root in roots:
        recurse(root, zero_out_sched)

    for source, opgroup in operations_grouped_by_source:
        source._ops_schedule = opgroup

    def compare(*ops_schedules):
        assert len(ops_schedules), "operations schedules cannot be empty: %r" % ops_schedules

        # in the case of the list of operations schedules being just one (no children)
        # we return True, cos it's safe to recursively replicate this one
        if len(ops_schedules) == 1:
            return True

        # now let's check that all ops schedules are the same length
        # otherwise they are not the same and we can say the comparison isn't the same
        lens = set([ len(o) for o in ops_schedules ])
        if len(lens) != 1:
            return False

        # we have multiple schedules
        # if their type, snapshot origin and snapshot destination are all the same
        # we can say that they are "the same"
        comparisons = [
                all([
                    # never attempt to recursivize operations who involve create_stub
                    all(["create_stub" not in o[0] for o in ops]),
                    len(set([o[0] for o in ops])) == 1,
                    any([o[3] is None for o in ops]) or len(set([o[3].name for o in ops])) == 1,
                    any([o[4] is None for o in ops]) or len(set([o[4].name for o in ops])) == 1,
                ])
                for ops
                in zip(*ops_schedules)
        ]
        return all(comparisons)

    # remove unnecessary stubs that stand in for only other stubs
    for root in roots:
        for dataset, _ in recurse(root, lambda d: d):
            ops = [z for x, y in recurse(dataset, lambda d: d._ops_schedule) for z in y]
            if all([o[0] == 'create_stub' for o in ops]):
                dataset._ops_schedule = []

    for root in roots:
        for dataset, _ in recurse(root, lambda d: d):
            if compare(*[y for x, y in recurse(dataset, lambda d: d._ops_schedule)]):
                old_ops_schedule = dataset._ops_schedule
                recurse(dataset, zero_out_sched)
                for op in old_ops_schedule:
                    dataset._ops_schedule.append((
                        op[0] + "_recursive", op[1], op[2], op[3], op[4]
                    ))

    new_operation_schedule = []
    for root in roots:
        for dataset, ops_schedule in recurse(root, lambda d: d._ops_schedule):
            new_operation_schedule.extend(ops_schedule)

    for root in roots:
        recurse(root, evict_sched)

    return new_operation_schedule

def optimize(operation_schedule, allow_recursivize = True):
    operation_schedule = optimize_coalesce(operation_schedule)
    if allow_recursivize:
        operation_schedule = optimize_recursivize(operation_schedule)
    return operation_schedule

# we walk the entire dataset structure, and sync snapshots recursively
def recursive_clear_obsolete(s, d):
    sched = []

    # we first collect all snapshot names, to later see if they are on both sides, one side, or what
    snapshots_in_src = set([ m.name for m in s.get_snapshots() ])
    snapshots_in_dst = set([ m.name for m in d.get_snapshots() ])

    snapshots_to_delete = snapshots_in_dst - snapshots_in_src
    snapshots_to_delete = [ d.get_snapshot(m) for m in snapshots_to_delete ]

    for m in snapshots_to_delete:
        sched.append(("destroy", m))

    # now let's apply the same argument to the children
    children_sched = []
    for child_d in [ x for x in d.children if not isinstance(x, Snapshot) ]:
        child_s = None

        try:
            child_s = s.get_child(child_d.name)
        except (KeyError, AttributeError):
            children_sched.append(("destroy_recursively", child_d))

        if child_s:
            children_sched.extend(recursive_clear_obsolete(child_s, child_d))

    # and return our schedule of operations to the parent
    return sched + children_sched
# END zfs-tools::sync.py



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



# =====================================================
# Begin new code for zfslib
# =====================================================


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


# Query all snapshots in Dataset allowing filter by: 
#  * name: Snapshot name (wildcard sup.), 
#  * dt_from: datetime to start
#  * tdelta: timedelta -or- str(nC) where: n is an integer > 0 and C is one of:
#  ** y=year, m=month, d=day, H=hour, M=minute, s=second. Eg 5H = 5 Hours
#  * dt_to: datetime to stop 
#  * Date search is any combination of (dt_from, dt_to) -or- (dt_from, tdelta) -or- (tdelta, dt_to)
def get_snapshots(ds:Dataset, name:str, dt_from:datetime=None, tdelta=None, dt_to:datetime=None) -> list:

    def __fil_n(snap):
        if not fnmatch.fnmatch(snap.name, name): return False
        return True

    def __fil_dt(snap):
        if not fnmatch.fnmatch(snap.name, name): return False
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
    
    return ds.get_snapshots(flt=f)




# Gets Diffs in snapshot or between snapshots (if snap_to is specified)
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
def get_diffs(conn:ZFSConnection, snap_from:Snapshot, snap_to:Snapshot=None, ignore:list=None, file_type:str=None, chg_type:str=None) -> list:
    if conn is None or not isinstance(conn, ZFSConnection):
        raise Exception("conn must be a ZFSConnection")
    if snap_from is None or not isinstance(snap_from, Snapshot):
        raise Exception("snap_from must be a Snapshot")
    if not snap_to is None and not isinstance(snap_to, Snapshot):
        raise Exception("snap_to must be a Snapshot")
    if not ignore is None and not isinstance(ignore, list):
        raise Exception("snap_to must be a Snapshot")

    cmd = conn.command + ["zfs", "diff", "-FHt", snap_from.get_path()]
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

