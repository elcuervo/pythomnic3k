#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module implements the central facility for request processing -
# a thread pool of fixed maximum size, allocating and deallocating threads
# as necessary.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "ThreadPool", "WorkUnitTimedOut" ]

################################################################################

import threading; from threading import Event, current_thread
import _thread; from _thread import exit
import sys; from sys import exc_info
import heapq; from heapq import heappush, heappop

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import typecheck, callable
import interlocked_queue; from interlocked_queue import InterlockedQueue, InterlockedPriorityQueue
import comparable_mixin; from comparable_mixin import ComparableMixin
import pmnc.timeout; from pmnc.timeout import Timeout
import pmnc.request; from pmnc.request import Request
import pmnc.resource_pool; from pmnc.resource_pool import Resource, \
       RegisteredResourcePool, ResourcePoolEmpty, ResourcePoolStopped
import pmnc.threads; from pmnc.threads import LightThread

################################################################################

class WorkUnitTimedOut(Exception): pass

################################################################################

class WorkUnit(ComparableMixin): # generic callable work unit with "earliest deadline first" order

    def __init__(self, request, f, args, kwargs):
        self._request, self._processed = request, Event()
        self._f, self._args, self._kwargs = f, args, kwargs

    def __lt__(self, other):
        return self._request < other._request

    def __eq__(self, other):
        return self._request == other._request

    def __call__(self):
        try:
            current_thread()._request = self._request
            self._result = self._f(*self._args, **self._kwargs)
        except Exception as e:
            self._result = e.with_traceback(exc_info()[2]) # capture the exception information
        finally:
            self._processed.set()

    def wait(self): # respects wall-time timeout, see issue9892
        if self._request.wait(self._processed): # inherits Request's behaviour
            if isinstance(self._result, Exception):
                raise self._result
            return self._result
        else:
            raise WorkUnitTimedOut("request deadline waiting for a work unit")

################################################################################

class PooledThread(Resource):

    def __init__(self, name, release):
        Resource.__init__(self, name)
        self._release = release
        self._ready, self._queue = Event(), InterlockedQueue()
        if __name__ == "__main__":
            self._timeout = Timeout(3.0)
        else:
            self._timeout = Timeout(60.0)
        self._count = 0

    def _expired(self):
        return self._timeout.expired or Resource._expired(self)

    def connect(self):
        Resource.connect(self)
        self._thread = LightThread(target = self._thread_proc,
                                   name = "{0:s}:?".format(self.name))
        self._thread.start()
        self._ready.wait(3.0) # this may spend waiting slightly less, but it's ok
        if not self._ready.is_set():
            self._queue.push(exit) # just in case the thread has in fact started
            raise Exception("new thread failed to start in 3.0 seconds")

    def _thread_proc(self):
        self._ready.set()
        while True: # exits upon processing of exit pushed in disconnect()
            try:
                self._count += 1
                thread_name = "{0:s}:{1:d}".format(self.name, self._count)
                current_thread().name = thread_name
                work_unit = self._queue.pop()
                work_unit()
                self._timeout.reset()
            finally:
                self._release(self) # this actually invokes ThreadPool._release

    # this method may be called by external thread (ex. pool sweep)
    # or by this thread itself, and posts an exit kind of work unit

    def disconnect(self):
        try:
            if current_thread() is not self._thread:
                self._queue.push(exit)
                self._thread.join(3.0)
        finally:
            Resource.disconnect(self)

    # this method is called by the thread pool to post a work unit
    # to this thread, as well as by the thread itself at disconnect

    def push(self, work_unit):
        self._queue.push(work_unit)

################################################################################

class ThreadPool:

    @typecheck
    def __init__(self, name: str, size: int):
        self._threads = RegisteredResourcePool(name, lambda name: PooledThread(name, self._release), size)
        self._queue = InterlockedPriorityQueue()

    name = property(lambda self: self._threads.name)
    size = property(lambda self: self._threads.size)
    free = property(lambda self: self._threads.free)
    busy = property(lambda self: self._threads.busy)
    over = property(lambda self: len(self._queue))

    def _push(self, work_unit = None):
        if work_unit:
            self._queue.push(work_unit)
        work_unit = self._queue.pop(0.0)
        if work_unit is None:
            return
        try:
            thread = self._threads.allocate()
        except (ResourcePoolEmpty, ResourcePoolStopped):
            self._queue.push(work_unit)
        else:
            thread.push(work_unit)

    # this method is magic - a thread previously allocated from the pool
    # releases itself back to it, then proceeds to allocate another thread,
    # possibly itself to keep processing while there are queued work units

    def _release(self, thread):
        self._threads.release(thread)
        self._push()

    valid_work_unit = lambda f: callable(f) and f.__name__.startswith("wu_")

    @typecheck
    def enqueue(self, request: Request, f: valid_work_unit, args: tuple, kwargs: dict):
        work_unit = WorkUnit(request, f, args, kwargs)
        self._push(work_unit)
        return work_unit

################################################################################

if __name__ == "__main__":

    print("self-testing module thread_pool.py:")

    from threading import Lock
    from exc_string import exc_string
    from expected import expected
    from time import time, sleep
    from functools import reduce
    from math import sqrt
    from typecheck import InputParameterError
    from pmnc.request import fake_request, InfiniteRequest

    ###################################

    def wu_skip(): pass

    def wu_loopback(*args, **kwargs):
        return args, kwargs

    ###################################

    print("work unit timeouts: ", end = "")

    wu = WorkUnit(fake_request(0.1), wu_skip, (), {})
    before = time()
    with expected(WorkUnitTimedOut):
        wu.wait()
    after = time()
    assert after - before >= 0.1

    wu = WorkUnit(fake_request(0.1), wu_skip, (), {}); wu()
    before = time()
    wu.wait()
    after = time()
    assert after - before < 0.01

    print("ok")

    ###################################

    print("earliest deadline first ordering: ", end = "")

    wu1 = WorkUnit(fake_request(1.0), wu_skip, (), {})
    wu2 = WorkUnit(fake_request(2.0), wu_skip, (), {})
    wuX = WorkUnit(InfiniteRequest(), wu_skip, (), {})

    assert wu1 == wu1 and wu1 < wu2 and wu2 == wu2 and wu2 < wuX and wuX == wuX
    assert (wu1 < wu2 < wuX) and (wu1 <= wu2 <= wuX)
    assert (wuX > wu2 > wu1) and (wuX >= wu2 >= wu1)
    assert wu1 != wu2 and wu2 != wuX and wu1 != wuX

    print("ok")

    ###################################

    print("single work unit: ", end = "")

    RegisteredResourcePool.start_pools(0.5)
    try:

        rq = fake_request(0.5)

        tp = ThreadPool("TP", 1)

        with expected(InputParameterError("enqueue() has got an incompatible value for f: ")):
            tp.enqueue(rq, lambda: None, (), {})

        r = tp.enqueue(rq, wu_loopback, ("foo", "bar"), {"biz": "baz"}).wait()
        assert r == (("foo", "bar"), { "biz": "baz" })

        def wu_get_thread_name():
            return current_thread().name

        r = tp.enqueue(rq, wu_get_thread_name, (), {}).wait()
        assert r == "TP/1:2"

        class FooException(Exception): pass

        def wu_failed():
            raise FooException("fails")

        try:
            tp.enqueue(rq, wu_failed, (), {}).wait()
        except FooException as e:
            assert exc_string().startswith("FooException(\"fails\") in wu_failed() (thread_pool.py:256)")
        else:
            assert False

        sleep(0.5)

        with expected(WorkUnitTimedOut("request deadline waiting for a work unit")):
            tp.enqueue(rq, wu_skip, (), {}).wait()

    finally:
        RegisteredResourcePool.stop_pools()

    print("ok")

    ###################################

    print("work unit ordering and cancellation: ", end = "")

    RegisteredResourcePool.start_pools(0.5)
    try:

        tp = ThreadPool("TP", 1)

        res = []

        def wu_sleep(t): sleep(t)
        def wu_append(s): res.append(s)

        wu0 = tp.enqueue(fake_request(1.0), wu_sleep, (0.5, ), {})
        wu1 = tp.enqueue(fake_request(1.5), wu_append, ("1", ), {})
        wu2 = tp.enqueue(fake_request(1.7), wu_append, ("2", ), {})
        wu3 = tp.enqueue(fake_request(1.2), wu_append, ("3", ), {})
        wu4 = tp.enqueue(fake_request(1.3), wu_append, ("4", ), {})

        wu0.wait(); wu1.wait(); wu2.wait(); wu3.wait(); wu4.wait()

        assert res == ["3", "4", "1", "2"]

        def wu_sleep_s(t, *, s):
            sleep(t)
            return s

        wu0 = tp.enqueue(fake_request(1.0), wu_sleep_s, (0.5, ), { "s" : "ok" })
        wu1 = tp.enqueue(fake_request(0.1), wu_skip, (), {})

        with expected(WorkUnitTimedOut("request deadline waiting for a work unit")):
            wu1.wait()

        assert wu0.wait() == "ok"

        wu2 = tp.enqueue(fake_request(0.0), wu_skip, (), {})

        with expected(WorkUnitTimedOut("request deadline waiting for a work unit")):
            wu2.wait()

    finally:
        RegisteredResourcePool.stop_pools()

    print("ok")

    ###################################

    print("exception capturing: ", end = "")

    RegisteredResourcePool.start_pools(0.5)
    try:

        rq = fake_request(0.5)

        def wu_error():
            1 / 0

        tp = ThreadPool("TP", 1)
        wu = tp.enqueue(rq, wu_error, (), {})

        try:
            wu.wait()
        except ZeroDivisionError:
            assert "in wu_error() (thread_pool.py:331)" in exc_string() # note the original line
        else:
            assert False

    finally:
        RegisteredResourcePool.stop_pools()

    print("ok")

    ###################################

    print("idle thread termination: ", end = "")

    RegisteredResourcePool.start_pools(0.5)
    try:

        rq = fake_request(0.5)

        tp = ThreadPool("TP", 1)

        tp.enqueue(rq, wu_skip, (), {}).wait()

        assert tp.free == 1 and tp.busy == 0
        sleep(1.25)
        assert tp.free == 1 and tp.busy == 0
        sleep(1.25)
        assert tp.free == 1 and tp.busy == 0
        sleep(1.25)
        assert tp.free == 0 and tp.busy == 0

    finally:
        RegisteredResourcePool.stop_pools()

    print("ok")

    ###################################

    print("single thread keeps processing: ", end = "")

    RegisteredResourcePool.start_pools(0.5)
    try:

        rq = fake_request(0.5)

        tp = ThreadPool("TP", 1)

        proc_res = []
        def wu_proc_i(i):
            def wu_proc():
                sleep(0.01)
                proc_res.append(i)
            return wu_proc
        procs = list(map(wu_proc_i, range(100)))
        wus = list(map(lambda pc: tp.enqueue(rq, pc, (), {}), procs))
        sleep(3.0)
        assert sorted(proc_res) == list(range(100))

    finally:
        RegisteredResourcePool.stop_pools()

    print("ok")

    ###################################

    print("two pools crossposting work: ", end = "")

    RegisteredResourcePool.start_pools(0.5)
    try:

        rq = fake_request(0.5)

        tpA = ThreadPool("TPA", 6)
        tpB = ThreadPool("TPB", 5)

        def wu_a(i):
            if i > 0:
                return i * tpB.enqueue(rq, wu_b, (i - 1,), {}).wait()
            else:
                return 1

        def wu_b(i):
            if i > 0:
                return i * tpA.enqueue(rq, wu_a, (i - 1,), {}).wait()
            else:
                return 1

        assert tpA.enqueue(rq, wu_a, (10,), {}).wait() == 3628800

    finally:
        RegisteredResourcePool.stop_pools()

    print("ok")

    ###################################

    print("multiple pools calculating factorial: ", end = "")

    RegisteredResourcePool.start_pools(0.5)
    try:

        rq = fake_request(3.0)

        def wu_factorial(n):
            if n == 1:
                return 1
            else:
                return n * ThreadPool("TP{0:d}".format(n), 1).enqueue(rq, wu_factorial, (n - 1,), {}).wait()

        assert wu_factorial(50) == reduce(lambda r, e: r * e, range(1, 51), 1)

    finally:
        RegisteredResourcePool.stop_pools()

    print("ok")

    ###################################

    print("avalanche processing, 10 threads: ", end = "")

    RegisteredResourcePool.start_pools(0.5)
    try:

        rq = fake_request(15.0)
        tp = ThreadPool("TP", 10)

        class Counter:
            def __init__(self):
                self._lock = Lock()
                self._count = 0
                self._counts = {}
            def inc(self):
                with self._lock:
                    self._counts.setdefault(current_thread(), 0)
                    self._counts[current_thread()] += 1
                    self._count += 1
                    return self._count
        c = Counter()

        def wu_foo():
            c.inc()
            tp.enqueue(rq, wu_foo, (), {})
            tp.enqueue(rq, wu_foo, (), {})

        tp.enqueue(rq, wu_foo, (), {})

        assert not rq.wait(Event())

        c = list(c._counts.values())
        n = len(c)
        assert 5 <= n <= 10
        s = sum(c)
        m = s / n
        assert m > 50
        sd = sqrt(sum((x - m) ** 2 for x in c) / n)
        df = list(map(lambda x: abs(x - m) / sd, c))
        assert max(df) < 3.0

    finally:
        RegisteredResourcePool.stop_pools()

    print("ok, {0:d} units/sec.".format(s // 15))

    ###################################

    print("all ok")

################################################################################
# EOF
