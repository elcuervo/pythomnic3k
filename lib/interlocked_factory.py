#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# Interlocked factory (counting created instances and allowing to wait for
# all existing instances to be destroyed).
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "InterlockedFactory" ]

################################################################################

import time; from time import time
import threading; from threading import Lock, Event

################################################################################

class InterlockedFactory:

    def __init__(self, factory):
        self._factory = factory
        self._stopped = Event()
        self._lock = Lock()
        self._count = 0
        self._zero = Event()
        self._zero.set()

    def _rcount(self):
        with self._lock:
            return self._count
    count = property(_rcount)

    def create(self, *args, **kwargs):
        with self._lock:
            if not self._stopped.is_set():
                result = self._factory(*args, **kwargs)
                self._count += 1
                self._zero.clear()
                return result, self._count
            else:
                return None

    def destroyed(self):
        with self._lock:
            self._count -= 1
            if self._count == 0:
                self._zero.set()
            return self._count

    def stop(self):
        with self._lock:
            assert not self._stopped.is_set()
            self._stopped.set()

    def wait(self, timeout): # respects wall-time timeout, see issue9892
        with self._lock:
            assert self._stopped.is_set() # the factory must be stopped
        start, remain = time(), timeout
        while remain > 0.0: # zero timeout => returns False immediately
            self._zero.wait(remain)
            if self._zero.is_set():
                return True
            remain = timeout - (time() - start)
        else:
            return False

################################################################################

if __name__ == "__main__":

    print("self-testing module interlocked_factory.py:")

    ###################################

    from expected import expected

    ###################################

    class Foo: pass

    ilf = InterlockedFactory(Foo)
    assert ilf.count == 0

    with expected(AssertionError):
        ilf.wait(1.0)

    f, n = ilf.create()
    assert isinstance(f, Foo)
    assert n == ilf.count == 1

    with expected(AssertionError):
        ilf.wait(1.0)

    with expected(TypeError):
        ilf.create("will throw")
    assert ilf.count == 1

    ilf.stop()
    with expected(AssertionError):
        ilf.stop()

    assert ilf.create() is None

    assert not ilf.wait(1.0)
    assert not ilf.wait(0.0)

    before = time()
    assert not ilf.wait(0.1)
    after = time()
    assert after - before >= 0.1

    n = ilf.destroyed()
    assert n == ilf.count == 0

    assert ilf.wait(1.0)
    assert not ilf.wait(0.0)

    ###################################

    print("ok")

################################################################################
# EOF
