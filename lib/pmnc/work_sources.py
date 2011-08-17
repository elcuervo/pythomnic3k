#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module implements an interlocked thread-safe structure for tracking
# incoming work from multiple sources and allocating processing of the work.
#
# wss = WorkSources(number_of_sources, idle_timeout)
#
# Specifically, producer threads can indicate presence of work at Nth source
# by calling
#
# wss.add_work(N)
#
# and consumer thread (which must be heavy) can poll for the presence of work
# at any source by calling
#
# (stopped, N) = wss.begin_work([timeout])
# ...use Nth source...
# wss.end_work(N)
#
# and the source cannot be emitted from begin_work again, until the client
# indicates end of processing by calling end_work.
#
# Two additional notes:
# 1. begin_work detects stopping of the calling thread and exits with boolean flag.
# 2. if any source didn't have any work for idle_timeout, it is nevertheless
#    returned from begin_work, presumably so that the caller can check the source
#    manually.
#
# This module is written specifically to suit protocol_retry.py's needs,
# therefore it is hardly useful anywhere else.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "WorkSources" ]

################################################################################

import threading; from threading import Lock, Event, current_thread

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import typecheck, optional
import pmnc.timeout; from pmnc.timeout import Timeout

################################################################################

class _WorkSource:

    @typecheck
    def __init__(self, idle_timeout: float):
        self._idle_timeout = Timeout(idle_timeout)
        self._has_work = False
        self._working = False

    def add_work(self):
        self._has_work = True

    def begin_work(self):
        if not self._working and (self._has_work or self._idle_timeout.expired):
            self._has_work = False
            self._working = True
            return True
        else:
            return False

    def end_work(self):
        assert self._working
        self._working = False
        self._idle_timeout.reset()

################################################################################

class WorkSources:

    @typecheck
    def __init__(self, size: int, idle_timeout: float):
        self._lock = Lock()
        self._signal = Event()
        self._idle_timeout = idle_timeout
        self._sources = tuple(_WorkSource(idle_timeout) for i in range(size))

    @typecheck
    def add_work(self, i: int):
        with self._lock:
            self._sources[i].add_work()
            self._signal.set()

    @typecheck
    def begin_work(self, timeout: optional(float) = None) -> (bool, optional(int)):
        timeout = Timeout(timeout or self._idle_timeout + 1.0)
        while not timeout.expired:
            if current_thread().stopped():
                return True, None
            self._signal.wait(min(timeout.remain, 3.0)) # this may spend waiting slightly less, but it's ok
            with self._lock:
                for i, source in enumerate(self._sources):
                    if source.begin_work():
                        return False, i
                else:
                    self._signal.clear()
        else:
            return False, None

    @typecheck
    def end_work(self, i: int):
        with self._lock:
            self._sources[i].end_work()
            self._signal.set()

################################################################################

if __name__ == "__main__":

    print("self-testing module work_sources.py:")

    ###################################

    from time import sleep
    from expected import expected
    from pmnc.threads import HeavyThread

    ###################################

    def test_WorkSource_idle():

        ws = _WorkSource(2.0)
        assert not ws.begin_work()
        sleep(1.2)
        assert not ws.begin_work()
        sleep(1.2)
        assert ws.begin_work()

        test_WorkSource_idle()

    ###################################

    def test_WorkSource_working():

        ws = _WorkSource(2.0)
        assert not ws.begin_work()
        ws.add_work()
        assert ws.begin_work()
        assert not ws.begin_work()
        sleep(2.4) # cannot go idle while working
        assert not ws.begin_work()
        ws.end_work()
        assert not ws.begin_work()

    test_WorkSource_working()

    ###################################

    def test_WorkSource_add_work():

        ws = _WorkSource(2.0)
        assert not ws.begin_work()
        ws.add_work() # multiple calls to add_work count as one
        ws.add_work()
        ws.add_work()
        assert ws.begin_work()
        assert not ws.begin_work() # working
        ws.end_work()
        assert not ws.begin_work() # all work has been accounted for
        ws.add_work()
        assert ws.begin_work()
        ws.add_work() # add_work while working will count later
        ws.end_work()
        assert ws.begin_work()

    test_WorkSource_add_work()

    ###################################

    def test_WorkSource_end_work():

        ws = _WorkSource(2.0)
        with expected(AssertionError):
            ws.end_work()
        ws.add_work()
        with expected(AssertionError):
            ws.end_work()
        assert ws.begin_work()
        ws.end_work()
        with expected(AssertionError):
            ws.end_work()

    test_WorkSource_end_work()

    ###################################

    def test_WorkSources_idle():

        current_thread().stopped = lambda: False

        wss = WorkSources(2, 2.0)

        assert wss.begin_work(1.2) == (False, None)
        assert wss.begin_work(1.2) == (False, 0)
        assert wss.begin_work(0.0) == (False, 1)
        assert wss.begin_work(0.0) == (False, None)
        assert wss.begin_work() == (False, None)

    test_WorkSources_idle()

    ###################################

    def test_WorkSources_add_work():

        current_thread().stopped = lambda: False

        wss = WorkSources(2, 2.0)

        wss.add_work(0)
        assert wss.begin_work(0.0) == (False, 0)
        assert wss.begin_work(1.2) == (False, None)
        assert wss.begin_work(1.2) == (False, 1)
        assert wss.begin_work(1.2) == (False, None)
        assert wss.begin_work(1.2) == (False, None)
        wss.add_work(0)
        wss.end_work(1)
        assert wss.begin_work(1.2) == (False, None)
        assert wss.begin_work(1.2) == (False, 1)
        wss.add_work(1)
        wss.end_work(1)
        assert wss.begin_work(0.0) == (False, 1)
        assert wss.begin_work(1.2) == (False, None)
        wss.end_work(0)
        assert wss.begin_work(0.0) == (False, 0)
        assert wss.begin_work(1.2) == (False, None)

    test_WorkSources_add_work()

    ###################################

    def test_WorkSources_stop_thread():

        th_started = Event()

        def th_proc():
            wss = WorkSources(1, 30.0)
            th_started.set()
            assert wss.begin_work(10.0) == (True, None)

        th = HeavyThread(target = th_proc)
        th.start()
        t = Timeout(30.0)
        th_started.wait()
        sleep(1.0)
        th.stop()
        assert t.remain > 25.0

    test_WorkSources_stop_thread()

    ###################################

    def test_WorkSources_timeout():

        current_thread().stopped = lambda: False

        wss = WorkSources(1, 30.0)
        t = Timeout(10.0)
        assert wss.begin_work(3.0) == (False, None)
        assert abs(t.remain - 7.0) < 1.0

    test_WorkSources_timeout()

    ###################################

    def test_WorkSources_signal_kept():

        current_thread().stopped = lambda: False

        wss = WorkSources(4, 30.0)
        wss.add_work(0)
        wss.add_work(1)
        wss.add_work(2)
        wss.add_work(3)
        t = Timeout(10.0)
        assert wss.begin_work(1.0) == (False, 0)
        assert wss.begin_work(1.0) == (False, 1)
        assert wss.begin_work(1.0) == (False, 2)
        assert wss.begin_work(1.0) == (False, 3)
        assert t.remain > 9.0
        assert wss.begin_work(3.0) == (False, None)
        assert t.remain < 8.0

    test_WorkSources_signal_kept()

    ###################################

    def test_WorkSources_signal_kick():

        th_started = Event()
        th_got_work = Event()
        wss = WorkSources(1, 30.0)

        def th_proc():
            th_started.set()
            assert wss.begin_work(10.0) == (False, 0)
            th_got_work.set()

        th = HeavyThread(target = th_proc)
        th.start()
        th_started.wait()
        t = Timeout(30.0)
        wss.add_work(0)
        th_got_work.wait()
        assert t.remain > 29.0
        th.stop()

    test_WorkSources_signal_kick()

    ###################################

    def test_WorkSources_end_work():

        current_thread().stopped = lambda: False

        wss = WorkSources(2, 30.0)
        wss.add_work(1)
        assert wss.begin_work(0.0) == (False, 1)
        wss.add_work(0)
        assert wss.begin_work(0.0) == (False, 0)
        assert wss.begin_work(0.0) == (False, None)
        wss.add_work(1)
        assert wss.begin_work(0.0) == (False, None)

        th_started = Event()
        th_got_work = Event()

        def th_proc():
            th_started.set()
            assert wss.begin_work(10.0) == (False, 1)
            th_got_work.set()

        th = HeavyThread(target = th_proc)
        th.start()
        th_started.wait()
        t = Timeout(30.0)
        wss.end_work(1)
        th_got_work.wait()
        assert t.remain > 29.0, t.remain
        th.stop()

    test_WorkSources_end_work()

    ###################################

    print("ok")

################################################################################
# EOF