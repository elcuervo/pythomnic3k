#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# Interlocked counter (sequencer) with optional wraparound.
#
# Pythomnic3k project
# (c) 2005-2009, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "InterlockedCounter" ]

################################################################################

import threading; from threading import Lock

################################################################################

class InterlockedCounter:

    def __init__(self, modulo = None):
        self._lock = Lock()
        self._count, self._modulo = 0, modulo

    def next(self):
        with self._lock:
            count = self._count
            self._count += 1
            if self._modulo is not None:
                self._count %= self._modulo
            return count

################################################################################

if __name__ == "__main__":

    print("self-testing module interlocked_counter.py:")

    ic = InterlockedCounter()
    for i in range(10000):
        assert ic.next() == i

    for modulo in range(1, 1001):
        ic = InterlockedCounter(modulo)
        for i in range(1000):
            assert ic.next() == i % modulo

    print("ok")

################################################################################
# EOF
