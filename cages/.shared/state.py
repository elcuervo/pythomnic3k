#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module implements module persistent state - a private BerkeleyDB database
# for every module, accessible via pmnc.state.
#
# For example, the following statements
#
# pmnc.state.set("key", value)
# pmnc.state.get("key", default)
# pmnc.state.delete("key")
#
# in module bar.py access its default private database found in $cage/state/bar.
#
# Any module can also allocate and use any number of private databases and/or
# persistent queues using get_database and get_queue methods and use them in
# transactions using implicit_transaction or start_transaction and explicit_transaction
# methods.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "start", "stop", "get_database", "get_queue", "get", "set", "delete",
            "implicit_transaction", "start_transaction", "explicit_transaction",
            "default_state_db" ]
__reloadable__ = False

###############################################################################

import os; from os import path as os_path, mkdir
import threading; from threading import Lock, current_thread
import errno; from errno import EEXIST
import pickle; from pickle import dumps as pickle, loads as unpickle
try:
    import bsddb3 as bsddb3; from bsddb3 import db as bsddb
except ImportError:
    _no_bssdb3 = True
else:
    _no_bssdb3 = False

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, callable, optional
import perf_info; from perf_info import get_free_disk_space
import pmnc.timeout; from pmnc.timeout import Timeout
import pmnc.threads; from pmnc.threads import HeavyThread

###############################################################################

# module-level state => not reloadable

_state_cache_lock = Lock()
_state_cache = {}
_state_thread = None
_enough_free_disk_space = False

###############################################################################

class ModuleState:

    @typecheck
    def __init__(self, dir: os_path.isdir):

        if _no_bssdb3:
            raise Exception("the state is not available")

        self._dir = dir

        # the berkeley db environment opened here will be used
        # for all the multiple databases of the module

        env = bsddb.DBEnv()
        env.set_tx_max(100)
        env.set_cachesize(0, 4194304)
        env.set_lg_bsize(131072)
        env.set_lg_max(4194304)
        env.set_lk_detect(bsddb.DB_LOCK_RANDOM)
        env.set_lk_max_locks(16384)
        env.set_lk_max_lockers(8192)
        env.set_lk_max_objects(8192)
        env.set_flags(bsddb.DB_AUTO_COMMIT | bsddb.DB_TXN_NOWAIT, 1)
        try:
            env.log_set_config(bsddb.DB_LOG_AUTO_REMOVE | bsddb.DB_LOG_DSYNC, 1)
        except AttributeError:
            pass # log_set_config is only supported in BerkeleyDB 4.7+
        else:
            try:
                env.log_set_config(bsddb.DB_LOG_DIRECT, 1)
            except bsddb.DBInvalidArgError:
                pass # DB_LOG_DIRECT may be unsupported in virtual machines
        env.open(self._dir, bsddb.DB_CREATE | bsddb.DB_RECOVER |
                            bsddb.DB_PRIVATE | bsddb.DB_THREAD |
                            bsddb.DB_INIT_LOCK | bsddb.DB_INIT_MPOOL |
                            bsddb.DB_INIT_TXN | bsddb.DB_INIT_LOG)
        self._env = env

        # the created databases will be cached here

        self._db_cache_lock = Lock()
        self._db_cache = {}

    ###################################

    def _get_db(self, name, type, **db_opts):
        with self._db_cache_lock:
            db = self._db_cache.get(name)
            if db is None:
                db = bsddb.DB(self._env)
                for n, v in db_opts.items():
                    getattr(db, "set_{0:s}".format(n))(v)
                db.open(os_path.join(self._dir, name), None, type,
                        bsddb.DB_CREATE | bsddb.DB_THREAD)
                self._db_cache[name] = db
            return db

    def get_btree_db(self, name, **db_opts):
        db_name = "{0:s}.btree".format(name)
        return self._get_db(db_name, bsddb.DB_BTREE, **db_opts)

    def get_queue_db(self, name, **db_opts):
        db_name = "{0:s}.queue".format(name)
        return self._get_db(db_name, bsddb.DB_QUEUE, **db_opts)

    ###################################

    def start_transaction(self):
        return self._env.txn_begin(None, bsddb.DB_TXN_NOWAIT)

    ###################################

    def checkpoint(self):
        self._env.txn_checkpoint(64, 1, 0)

    ###################################

    def close(self):
        with self._db_cache_lock:
            for db in self._db_cache.values():
                try:
                    db.close()
                except:
                    pmnc.log.error(exc_string()) # log and ignore
            self._db_cache.clear()
        try:
            self._env.close()
        except:
            pmnc.log.error(exc_string()) # log and ignore

###############################################################################

@typecheck
def _get_module_state(module_name: str) -> ModuleState:

    if _no_bssdb3:
        raise Exception("the state is not available")

    if not _state_thread.is_alive():
        raise Exception("the state is no longer available")

    if not _enough_free_disk_space:
        raise Exception("the state is temporarily disabled "
                        "due to lack of free disk space")

    with _state_cache_lock:
        module_state = _state_cache.get(module_name)
        if not module_state:
            module_state = ModuleState(_module_state_dir(module_name))
            _state_cache[module_name] = module_state

    return module_state

###############################################################################

def _ensure_directory_exists(dir):

    if not os_path.isdir(dir):
        try:
            mkdir(dir)
        except OSError as e:
            if e.errno != EEXIST:
                raise

###############################################################################

@typecheck
def _module_state_dir(module_name: str) -> os_path.isdir:

    cage_state_dir = os_path.join(__cage_dir__, "state")
    _ensure_directory_exists(cage_state_dir)

    module_state_dir = os_path.join(cage_state_dir, module_name)
    _ensure_directory_exists(module_state_dir)

    return module_state_dir

###############################################################################

def _checkpoint_state():

    with _state_cache_lock:
        for module_state in _state_cache.values():
            try:
                module_state.checkpoint()
            except:
                pmnc.log.error(exc_string()) # log and ignore

###############################################################################

def _check_free_disk_space():

    global _enough_free_disk_space

    cage_state_dir = os_path.join(__cage_dir__, "state")
    if os_path.isdir(cage_state_dir):
        free_disk_space = get_free_disk_space(cage_state_dir)
    else:
        free_disk_space = get_free_disk_space(__cage_dir__)

    _enough_free_disk_space = free_disk_space is None or \
        free_disk_space >= pmnc.config.get("minimum_free_space")

###############################################################################

def _shutdown_state():

    with _state_cache_lock:
        for module_state in _state_cache.values():
            try:
                module_state.close()
            except:
                pmnc.log.error(exc_string()) # log and ignore
        _state_cache.clear()

###############################################################################

def _state_thread_proc():

    _check_free_disk_space()

    checkpoint_interval = pmnc.config.get("checkpoint_interval")

    while not current_thread().stopped(checkpoint_interval):
        try:
            _checkpoint_state()
            _check_free_disk_space()
        except:
            pmnc.log.error(exc_string()) # log and ignore

    try:
        _shutdown_state()
    except:
        pmnc.log.error(exc_string()) # log and ignore

###############################################################################
# the following methods are called from ./shared/startup.py
# once when the cage is started or stopped

def start():
    global _state_thread
    if not _no_bssdb3:
        _state_thread = HeavyThread(target = _state_thread_proc,
                                    name = "state")
        _state_thread.start()
    else:
        pmnc.log.message("module bsddb3 could not be imported, "
                         "persistent state is disabled")

def stop():
    if _state_thread:
        _state_thread.stop()

###############################################################################
# the following methods allocate private databases for the calling module

def get_database(name: str, *, __source_module_name,
                 pagesize: optional(int) = 8192, **db_opts):

    db_opts["pagesize"] = pagesize

    module_state = _get_module_state(__source_module_name)
    return module_state.get_btree_db(name, **db_opts)

def get_queue(name: str, *, __source_module_name, re_len: int,
              pagesize: optional(int) = 32768, q_extentsize: optional(int) = 128,
              re_pad: optional(int) = 0x00, **db_opts):

    db_opts["re_len"] = re_len
    db_opts["pagesize"] = pagesize
    db_opts["q_extentsize"] = q_extentsize
    db_opts["re_pad"] = re_pad

    module_state = _get_module_state(__source_module_name)
    return module_state.get_queue_db(name, **db_opts)

###############################################################################
# this module's own get/set/delete use the calling module's state database

def default_state_db(source_module_name):
    return get_database("state", __source_module_name = source_module_name)

###############################################################################

def _set(txn, db, key, value):
    db.put(key.encode("unicode-escape"), pickle(value), txn)

def set(key: str, value, *, __source_module_name):
    db = default_state_db(__source_module_name)
    implicit_transaction(_set, db, key, value,
                         __source_module_name = __source_module_name)

###############################################################################

def _get(txn, db, key, default):
    value = db.get(key.encode("unicode-escape"), None, txn)
    if value is None:
        return default
    else:
        return unpickle(value)

def get(key: str, default = None, *, __source_module_name):
    db = default_state_db(__source_module_name)
    return implicit_transaction(_get, db, key, default,
                                __source_module_name = __source_module_name)

###############################################################################

def _delete(txn, db, key):
    db.delete(key.encode("unicode-escape"), txn)

def delete(key: str, *, __source_module_name):
    db = default_state_db(__source_module_name)
    implicit_transaction(_delete, db, key,
                         __source_module_name = __source_module_name)

###############################################################################
# this method attempts to execute the given content entirely within a transaction

def implicit_transaction(content: callable, *args, __source_module_name):

    return _implicit_transaction(content, __source_module_name, *args)

###############################################################################

def _implicit_transaction(content, source_module_name, *args):

    module_state = _get_module_state(source_module_name)
    txn = module_state.start_transaction()
    try:
        while True: # exits upon success or request deadline
            try:
                result = content(txn, *args)
            except bsddb.DBLockDeadlockError:
                txn.abort(); txn = None
                if pmnc.request.expired:
                    raise Exception("request deadline waiting for access to state")
                _handle_deadlock_retry()
                txn = module_state.start_transaction()
            else:
                break
    except:
        if txn: txn.abort()
        raise
    else:
        txn.commit()
        return result

###############################################################################
# this method attempts to execute the given content within a given transaction,
# if deadlock occurs, the transaction is aborted, a new one is created and
# returned uncommitted

def explicit_transaction(source_module_name: str, _txn, content: callable, *args):

    module_state = _get_module_state(source_module_name)
    txn = _txn
    while not pmnc.request.expired:
        try:
            result = content(txn, *args)
        except bsddb.DBLockDeadlockError:
            txn.abort()
            _handle_deadlock_retry()
            txn = module_state.start_transaction()
        else:
            return txn, result # note that the returned transaction may be different
    else:
        if txn is not _txn:
            txn.abort()
        raise Exception("request deadline waiting for access to state")

###############################################################################
# this method is called whenever a deadlock occurs, it appears from testing
# that under stress a deadlocked transaction has good chances of succeeding
# if it is retried in at least 20 ms, and there seems to be no benefit in
# exponential backoff

def _handle_deadlock_retry(): # respects wall-time timeout, see issue9892
    Timeout(0.05).wait() # inherits Timeout's behaviour

###############################################################################
# this method initiates a state transaction and returns it uncommitted
# for the caller to use later with explicit_transaction

def start_transaction(source_module_name: str):

    module_state = _get_module_state(source_module_name)
    txn = module_state.start_transaction()
    return txn, _commit_transaction, _rollback_transaction

###############################################################################
# direct references to the following methods are given out by the start_transaction
# method to make the calls as brief and unlikely to fail as possible

def _commit_transaction(txn):
    txn.commit()

def _rollback_transaction(txn):
    try:
        txn.abort()
    except bsddb.DBError:
        pass # ignored, because abort might have already been called

###############################################################################

def self_test():

    from expected import expected
    from os import urandom, listdir
    from random import randint
    from time import time
    from threading import Thread, Event
    from pmnc.request import fake_request
    from typecheck import by_regex, InputParameterError
    from interlocked_counter import InterlockedCounter

    rus = "\u0410\u0411\u0412\u0413\u0414\u0415\u0401\u0416\u0417\u0418\u0419" \
          "\u041a\u041b\u041c\u041d\u041e\u041f\u0420\u0421\u0422\u0423\u0424" \
          "\u0425\u0426\u0427\u0428\u0429\u042c\u042b\u042a\u042d\u042e\u042f" \
          "\u0430\u0431\u0432\u0433\u0434\u0435\u0451\u0436\u0437\u0438\u0439" \
          "\u043a\u043b\u043c\u043d\u043e\u043f\u0440\u0441\u0442\u0443\u0444" \
          "\u0445\u0446\u0447\u0448\u0449\u044c\u044b\u044a\u044d\u044e\u044f"

    rus_b = rus.encode("windows-1251")

    ###################################

    def _push(txn, db, value, f):
        return db.append(f(value), txn)

    def push(q, value, f = pickle):
        pmnc.state.implicit_transaction(_push, q, value, f)

    ###################################

    def _pop(txn, db, default, f):
        value = db.consume(txn)
        if value is not None:
            return f(value[1])
        else:
            return default

    def pop(q, default = None, f = unpickle):
        return pmnc.state.implicit_transaction(_pop, q, default, f)

    ###################################

    def _peek(txn, q, f):
        crs = q.cursor(txn)
        try:
            queue_item = crs.first()
            if queue_item is not None:
                return f(queue_item[1])
        finally:
            crs.close()

    def peek(q, f = unpickle):
        return pmnc.state.implicit_transaction(_peek, q, f)

    ###################################

    def test_module_state():

        fake_request(10.0)

        module_state_dir = _module_state_dir("foo")

        module_state = ModuleState(module_state_dir)
        try:

            # note that the first access to a queue or a database creates it,
            # therefore options such as re_len are required, but subsequent
            # accesses just open it and the options can be omitted

            assert not os_path.exists(os_path.join(module_state_dir, "state.queue"))
            queue = module_state.get_queue_db("state", re_len = 100)
            assert os_path.exists(os_path.join(module_state_dir, "state.queue"))
            stat = queue.stat(); assert "buckets" not in stat
            queue.append("foo")
            assert module_state.get_queue_db("state") is queue
            assert module_state.get_queue_db("state2") is not queue

            assert not os_path.exists(os_path.join(module_state_dir, "state.btree"))
            btree = module_state.get_btree_db("state")
            assert os_path.exists(os_path.join(module_state_dir, "state.btree"))
            stat = btree.stat(); assert "buckets" not in stat
            with expected(bsddb.DBInvalidArgError):
                btree.append("foo")
            assert module_state.get_btree_db("state") is btree
            assert module_state.get_btree_db("state2") is not btree

        finally:
            module_state.close()

    test_module_state()

    ###################################

    def test_module_state_cache():

        module_state = _get_module_state("foo")
        assert isinstance(module_state, ModuleState)
        assert _get_module_state("foo") is module_state
        assert _get_module_state("bar") is not module_state

    test_module_state_cache()

    ###################################

    def test_queue():

        fake_request(10.0)

        q = pmnc.state.get_queue("some_queue", re_len = 1024)

        assert pop(q, "default") == "default"
        push(q, None)
        assert pop(q, "default") is None
        assert pop(q, "default") == "default"

        # and now for str/bytes support

        push(q, rus)
        assert pop(q) == rus

        push(q, rus_b)
        assert pop(q) == rus_b

    test_queue()

    ###################################

    def test_queue_many_items():

        fake_request(60.0)

        q = pmnc.state.get_queue("queue_many_items", re_len = 64)

        counter = InterlockedCounter()

        def _push_n(txn, n):
            for i in range(n):
                c = counter.next()
                _push(txn, q, "test-{0:d}".format(c), pickle)

        def push_n(n):
            pmnc.state.implicit_transaction(_push_n, n)

        # push as much as we can per single transaction

        for i in range(8):
            push_n(1024)
            push_n(2048)
            push_n(4096)
            push_n(8100)

        # until we hit the limit of max_objects (8192) for one transaction

        with expected(MemoryError):
            push_n(8192)

        Timeout(4.0).wait() # wait for checkpoint

        # now pop everything off the queue

        def _pop_n(txn, n):
            for i in range(n):
                assert _pop(txn, q, None, unpickle) is not None

        def pop_n(n):
            pmnc.state.implicit_transaction(_pop_n, n)

        assert peek(q) == "test-0"

        for i in range(8):
            pop_n(8100)
            pop_n(4096)
            pop_n(2048)
            pop_n(1024)

        # the attempt to push 8192 records should have left no trace

        assert peek(q) is None

    test_queue_many_items()

    ###################################

    def test_queue_rollback_disorder():

        fake_request(10.0)

        q = pmnc.state.get_queue("queue_abort_reverse", re_len = 128)

        push(q, 1)
        push(q, 2)

        # first reader rolls back

        evt1 = Event()
        def txn1(txn):
            assert _pop(txn, q, None, unpickle) == 1
            evt1.set()
            evt2.wait()
            1 / 0 # this causes rollback of the first transaction

        # second reader commits

        evt2 = Event()
        def txn2(txn):
            evt1.wait()
            result = _pop(txn, q, None, unpickle)
            evt2.set()
            return result

        def th_proc():
            fake_request(10.0)
            with expected(ZeroDivisionError):
                pmnc.state.implicit_transaction(txn1)

        # in presence of multiple failing readers the order is not preserved

        th = Thread(target = th_proc)
        th.start()
        try:
            assert pmnc.state.implicit_transaction(txn2) == 2
        finally:
            th.join()

        assert pmnc.state.implicit_transaction(txn2) == 1

    test_queue_rollback_disorder()

    ###################################

    def test_queue_extent_reuse():

        fake_request(10.0)

        def current_extents():
            return sorted([ int(s.split(".")[-1])
                            for s in listdir(_module_state_dir(__name__))
                            if by_regex("^.*\\.ext_queue\\.queue\\.[0-9]+$")(s) ])

        q = pmnc.state.get_queue("ext_queue", pagesize = 8192, re_len = 1024, q_extentsize = 2)

        v = 0
        for i in range(64): # fill up more than one extent
            push(q, v)
            v += 1

        extents_before = current_extents()
        assert len(extents_before) > 1

        w = 0
        t = Timeout(5.0)
        while not t.expired: # keep popping and pushing to have the extents reused
            push(q, v)
            v += 1
            assert pop(q) == w
            w += 1

        extents_after = current_extents()
        assert extents_before[0] < extents_after[0]

    test_queue_extent_reuse()

    ###################################

    def test_default_state():

        fake_request(10.0)

        assert pmnc.state.get("") is None
        assert pmnc.state.get("", "default") == "default"

        pmnc.state.set("", None)
        assert pmnc.state.get("") is None
        assert pmnc.state.get("", "default") is None

        pmnc.state.delete("")
        assert pmnc.state.get("", "default") == "default"

        # and now for str/bytes support

        pmnc.state.set(rus, rus + rus)
        assert pmnc.state.get(rus) == rus + rus
        pmnc.state.delete(rus)

        with expected(InputParameterError):
            pmnc.state.set(rus_b, rus)

        with expected(InputParameterError):
            pmnc.state.get(rus_b)

        with expected(InputParameterError):
            pmnc.state.delete(rus_b)

        pmnc.state.set(rus, rus_b)
        assert pmnc.state.get(rus) == rus_b
        pmnc.state.delete(rus)

    test_default_state()

    ###################################

    def test_mixed_access():

        fake_request(10.0)

        pmnc.state.set("MIX-KEY", "VALUE")

        xa = pmnc.transaction.create()
        xa.state.get("MIX-KEY")
        assert xa.execute() == ("VALUE", )

        xa = pmnc.transaction.create()
        xa.state.delete("MIX-KEY")
        assert xa.execute() == (None, )

        assert pmnc.state.get("MIX-KEY") is None

        xa = pmnc.transaction.create()
        xa.state.set("MIX-KEY", "VALUE")
        assert xa.execute() == (None, )

        assert pmnc.state.get("MIX-KEY") == "VALUE"

    test_mixed_access()

    ###################################

    def test_large_keys():

        for i in range(16):
            key = "*" * (1 << i)
            pmnc.state.set(key, i)

        for i in range(16):
            key = "*" * (1 << i)
            assert pmnc.state.get(key) == i

    test_large_keys()

    ###################################

    def test_large_values():

        fake_request(10.0)

        for i in range(20):
            value = "*" * (1 << i)
            pmnc.state.set(str(i), value)

        for i in range(20):
            value = "*" * (1 << i)
            assert pmnc.state.get(str(i)) == value

    test_large_values()

    ###################################

    def test_large_queue_items():

        fake_request(10.0)

        q = pmnc.state.get_queue("large_items", re_len = 1024, re_pad = 0x5a)

        for i in (16, 64, 256, 1024):
            value = b"*" * i
            push(q, value, lambda x: x)

        with expected(bsddb.DBInvalidArgError):
            push(q, b"*" * 1025, lambda x: x)

        for i in (16, 64, 256, 1024):
            value = b"*" * i + b"\x5a" * (1024 - i)
            assert pop(q, None, lambda x: x) == value

        assert pop(q, None, lambda x: x) is None

    test_large_queue_items()

    ###################################

    def test_secondary_index():

        fake_request(30.0)

        def _insert(txn, db, idx, primary_key, secondary_key, value):
            db.put(primary_key.encode("unicode-escape"), pickle(value), txn)
            idx.put(secondary_key.encode("unicode-escape"), pickle(primary_key), txn)

        def _lookup_primary_key(txn, db, primary_key):
            value = db.get(primary_key.encode("unicode-escape"), None, txn)
            if value is not None:
                return unpickle(value)

        def _lookup_secondary_key(txn, db, idx, secondary_key):
            primary_key = idx.get(secondary_key.encode("unicode-escape"), None, txn)
            if primary_key is not None:
                return _lookup_primary_key(txn, db, unpickle(primary_key))

        N = 100
        pks = [ str(i) for i in range(N) ]
        sks = [ str(urandom(8)) for i in range(N) ]
        values = [ urandom(randint(1, 2048)) for i in range(N) ]

        # populate the database

        db = pmnc.state.get_database("indexed_database")
        idx = pmnc.state.get_database("indexed_database")

        start = time()
        for i in range(N):
            pmnc.state.implicit_transaction(_insert, db, idx, pks[i], sks[i], values[i])
        pmnc.log.info("indexed state: {0:d} insert(s)/sec.".format(int(N / ((time() - start) or 0.01))))

        # lookup by the primary index

        db = pmnc.state.get_database("indexed_database")

        start = time()
        for i in range(N):
            assert pmnc.state.implicit_transaction(_lookup_primary_key, db, pks[i]) == values[i]
        pmnc.log.info("indexed state: {0:d} primary lookup(s)/sec.".format(int(N / ((time() - start) or 0.01))))

        assert pmnc.state.implicit_transaction(_lookup_primary_key, db, "") is None

        # lookup by the primary index

        db = pmnc.state.get_database("indexed_database")
        idx = pmnc.state.get_database("indexed_database")

        start = time()
        for i in range(N):
            assert pmnc.state.implicit_transaction(_lookup_secondary_key, db, idx, sks[i]) == values[i]
        pmnc.log.info("indexed state: {0:d} secondary lookup(s)/sec.".format(int(N / ((time() - start) or 0.01))))

        assert pmnc.state.implicit_transaction(_lookup_secondary_key, db, idx, "") is None

    test_secondary_index()

    ###################################

    def test_queue_to_queue():

        fake_request(30.0)

        def _move(txn, q1, q2):
            value = _pop(txn, q1, None, unpickle)
            if value is not None:
                _push(txn, q2, value, pickle)
                return value
            else:
                return None

        def move(q1, q2):
            return pmnc.state.implicit_transaction(_move, q1, q2)

        q1 = pmnc.state.get_queue("queue1", re_len = 6144)
        q2 = pmnc.state.get_queue("queue2", re_len = 6144)

        N = 100
        values = [ urandom(2 ** (i % 12)) for i in range(N) ]

        # populate the first queue

        start = time()
        for i in range(N):
            push(q1, values[i])
        pmnc.log.info("queue: {0:d} push(es)/sec.".format(int(N / ((time() - start) or 0.01))))

        # then move items to the second queue

        start = time()
        while move(q1, q2) is not None:
            pass
        pmnc.log.info("queue-to-queue: {0:d} move(s)/sec.".format(int(N / ((time() - start) or 0.01))))

        # then pop it from the second queue

        start = time()
        for i in range(N):
            assert pop(q2) == values[i]
        pmnc.log.info("queue: {0:d} pop(s)/sec.".format(int(N / ((time() - start) or 0.01))))

        # now see if a single item can be moved from a queue to itself

        q3 = pmnc.state.get_queue("queue3", re_len = 6144)

        value = " " * (1 << 12)
        push(q3, value)

        start = time()
        for i in range(10):
            assert move(q3, q3) == value
        pmnc.log.info("large queue-to-itself: {0:d} move(s)/sec.".format(int(10 / ((time() - start) or 0.01))))

        q4 = pmnc.state.get_queue("queue4", re_len = 6144)

        value = " "
        push(q4, value)

        start = time()
        for i in range(100):
            assert move(q4, q4) == value
        pmnc.log.info("small queue-to-itself: {0:d} move(s)/sec.".format(int(100 / ((time() - start) or 0.01))))

    test_queue_to_queue()

    ###################################

    def test_threads():

        ldb = pmnc.state.get_database("stress_left_db")
        rdb = pmnc.state.get_database("stress_right_db")
        lq = pmnc.state.get_queue("stress_queue_left", re_len = 6144)
        rq = pmnc.state.get_queue("stress_queue_right", re_len = 6144)

        data_lock = Lock()
        left_data = {}
        right_data = {}

        def register(d, k, v):
            with data_lock:
                d[k] = v

        def th_proc(n):

            count = 0

            def next_key():
                nonlocal count
                count += 1
                return "{0:d}_{1:d}".format(n, count)

            def next_value():
                return urandom(randint(1, 2048))

            while count < 10:

                fake_request(10.0)

                left_value = pop(lq)
                if left_value is not None:
                    key, value = next_key(), left_value
                    pmnc.state.implicit_transaction(_set, ldb, key, value)
                    register(left_data, key, value)
                    push(rq, left_value)
                push(rq, next_value())

                right_value = pop(rq)
                if right_value is not None:
                    key, value = next_key(), right_value
                    pmnc.state.implicit_transaction(_set, rdb, key, value)
                    register(right_data, key, value)
                    push(lq, right_value)
                push(lq, next_value())

            while True:

                fake_request(10.0)

                left_value = pop(lq)
                if left_value is not None:
                    key, value = next_key(), left_value
                    pmnc.state.implicit_transaction(_set, ldb, key, value)
                    register(left_data, key, value)
                else:
                    break

            while True:

                fake_request(10.0)

                right_value = pop(rq)
                if right_value is not None:
                    key, value = next_key(), right_value
                    pmnc.state.implicit_transaction(_set, rdb, key, value)
                    register(right_data, key, value)
                else:
                    break

        N = 10
        ths = [ Thread(target = th_proc, args = (i, )) for i in range(N) ]
        for th in ths: th.start()
        for th in ths: th.join()

        for key, value in left_data.items():
            assert value == pmnc.state.implicit_transaction(_get, ldb, key, None)

        for key, value in right_data.items():
            assert value == pmnc.state.implicit_transaction(_get, rdb, key, None)

    test_threads()

    ###################################

    def test_log_truncation():

        def current_logs():
            return sorted([ int(s[4:])
                            for s in listdir(_module_state_dir(__name__))
                            if s.startswith("log.") ])

        def grow_log():
            for i in range(32):
                pmnc.state.set(str(randint(0, 127)), urandom(65536))
            return current_logs()

        fake_request(60.0)

        logs_before = grow_log()
        logs_after = grow_log()
        while logs_before[0] == logs_after[0]:
            Timeout(1.0).wait()
            logs_after = grow_log()

        assert logs_before[0] < logs_after[0]


    if hasattr(_get_module_state("foo")._env, "log_set_config"):
        test_log_truncation()

    ###################################

    def test_cross_db_deadlock():

        fake_request(30.0)

        db1 = pmnc.state.get_database("db1")
        db2 = pmnc.state.get_queue("db2", re_len = 6144)

        def f(txn):
            db1.put(b"key", b"value_1", txn)
            Timeout(3.0).wait()
            db2.append(pickle("item_1"), txn)

        def g(txn):
            db2.append(pickle("item_2"), txn)
            Timeout(3.0).wait()
            db1.put(b"key", b"value_2", txn)

        th_f = HeavyThread(target = lambda: pmnc.state.implicit_transaction(f))
        th_g = HeavyThread(target = lambda: pmnc.state.implicit_transaction(g))

        th_f.start()
        th_g.start()

        th_f.join()
        th_g.join()

        # now see what went through

        def fetch_result(txn):
            value = db1.get(b"key", None, txn)
            item1 = _pop(txn, db2, None, unpickle)
            item2 = _pop(txn, db2, None, unpickle)
            return value, item1, item2

        value, item1, item2 = pmnc.state.implicit_transaction(fetch_result)
        assert value in (b"value_1", b"value_2")
        assert (item1, item2) in (("item_1", "item_2"), ("item_2", "item_1"))

    test_cross_db_deadlock()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
