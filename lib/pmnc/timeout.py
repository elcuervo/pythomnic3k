#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module implements a simple class encapsulating a timeout concept.
# Timeout can be created, reset and used for waiting for an event or a queue
# for a limited amount of time.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "Timeout" ]

################################################################################

import threading; from threading import Lock, Event
import time; from time import time
import random; from random import normalvariate

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import typecheck, optional
import interlocked_queue; from interlocked_queue import InterlockedQueue

################################################################################

class Timeout:

    @typecheck
    def __init__(self, timeout: float):
        self._timeout = timeout
        self._lock = Lock()
        self.reset()

    timeout = property(lambda self: self._timeout)
    def _rremain(self):
        with self._lock:
            return max(self._deadline - time(), 0.0)
    remain = property(lambda self: self._rremain())

    @typecheck
    def reset(self, timeout: optional(float) = None):
        with self._lock:
            if timeout is not None:
                self._timeout = timeout
            self._deadline = time() + self._timeout

    def _expired(self):
        with self._lock:
            return time() >= self._deadline
    expired = property(lambda self: self._expired())

    _never_set = Event()

    @typecheck
    def wait(self, event: optional(Event) = None) -> bool: # respects wall-time timeout, see issue9892
        remain, event = self.remain, event or self._never_set
        while remain > 0.0:
            event.wait(remain)
            if event.is_set():
                return True
            else:
                remain = self.remain
        else:
            return False

    @typecheck
    def pop(self, queue: InterlockedQueue): # respects wall-time timeout, see issue9892
        return queue.pop(self.remain) # inherits InterlockedQueue's behaviour

################################################################################

if __name__ == "__main__":

    print("self-testing module timeout.py:")

    from threading import Event
    from time import sleep

    from typecheck import InputParameterError
    from expected import expected

    ###################################

    t = Timeout(2.0)
    assert t.remain > 1.9
    sleep(1.25)
    assert t.remain > 0.5
    assert not t.expired
    sleep(1.25)
    assert t.remain == 0.0
    assert t.expired

    t = Timeout(2.0)
    sleep(1.25)
    assert not t.expired
    t.reset()
    assert t.remain > 1.9
    sleep(1.25)
    assert not t.expired

    with expected(InputParameterError("__init__() has got an incompatible value for timeout: 1")):
        Timeout(1)

    before = time()
    t = Timeout(1.0)
    while not t.expired:
        pass
    after = time()
    assert after - before >= 1.0

    ###################################

    t = Timeout(0.5)
    sleep(1.0)
    assert t.expired
    t.reset()
    sleep(1.0)
    assert t.expired
    t.reset(1.5)
    sleep(1.0)
    assert not t.expired
    sleep(1.0)
    assert t.expired

    ###################################

    t = Timeout(1.0)
    e = Event()
    assert not t.wait(e) and t.expired
    e.set()
    assert not t.wait(e)

    t = Timeout(1.0)
    assert t.wait(e) and not t.expired

    t = Timeout(0.5)
    assert not t.wait() and t.expired

    before = time()
    Timeout(0.1).wait()
    after = time()
    assert after - before >= 0.1

    ###################################

    ilq = InterlockedQueue()

    t = Timeout(0.1)

    before = time()
    assert t.pop(ilq) is None
    after = time()
    assert after - before >= 0.1
    assert t.expired

    t.reset(0.1)
    ilq.push(1)

    before = time()
    assert t.pop(ilq) == 1
    after = time()
    assert after - before < 0.01
    assert not t.expired

    ###################################

    print("ok")

################################################################################
# EOF
