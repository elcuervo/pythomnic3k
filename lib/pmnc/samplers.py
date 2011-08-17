#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module implements classes that receive streams of readings and update
# running statistical parameters, such as average for the few last seconds.
# Instances of these classes are used by module performance.py to keep cage
# runtime performance statistics.
#
# RawSampler tracks measurable facts and calculates average value etc.
# RateSampler tracks atomic events and calculates their rate of arrival.
#
# Pythomnic3k project
# (c) 2005-2009, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "RawSampler", "RateSampler" ]

###############################################################################

import threading; from threading import Lock
import time; from time import time
import math; from math import sqrt

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import typecheck

###############################################################################

class RawSampler:

    @typecheck
    def __init__(self, period: float):
        self._lock = Lock()
        self._period = period
        self._clear()

    def _rcount(self):
        with self._lock:
            self.trim()
            return self._count

    def _rmin(self):
        with self._lock:
            self.trim()
            return self._min

    def _rmax(self):
        with self._lock:
            self.trim()
            return self._max

    def _ravg(self):
        with self._lock:
            self.trim()
            return self._sum / (self._count or 1)

    def _rdev(self):
        with self._lock:
            self.trim()
            n = (self._count or 1)
            avg = self._sum / n
            return sqrt(sum((data[1] - avg) ** 2
                             for data in self._data)
                        / n)

    count = property(lambda self: self._rcount())
    min = property(lambda self: self._rmin())
    max = property(lambda self: self._rmax())
    avg = property(lambda self: self._ravg())
    dev = property(lambda self: self._rdev())

    def trim(self):
        deadline = time() - self._period
        recalc_min, recalc_max = False, False
        while self._data and self._data[0][0] <= deadline:
            data = self._data.pop(0)[1]
            self._count -= 1
            self._sum -= data
            if data == self._min: recalc_min = True
            if data == self._max: recalc_max = True
        if recalc_min:
            if self._data:
                self._min = min(map(lambda t_d: t_d[1], self._data))
            else:
                self._min = 2**63-1
        if recalc_max:
            if self._data:
                self._max = max(map(lambda t_d: t_d[1], self._data))
            else:
                self._max = -2**63

    def _iadd(self, data):
        self._count += 1
        self._sum += data
        self._min = min(self._min, data)
        self._max = max(self._max, data)
        self._data.append((time(), data))

    @typecheck
    def __iadd__(self, data: int):
        with self._lock:
            self._iadd(data)
            if self._count % 100 == 0:
                self.trim()
        return self

    def _clear(self):
        self._data, self._count, self._sum = [], 0, 0
        self._min, self._max = 2**63-1, -2**63

    def clear(self):
        with self._lock:
            self._clear()

################################################################################

class RateSampler(RawSampler):

    def tick(self):
        with self._lock:
            self._ticks.append(time())
            if len(self._ticks) % 100 == 0:
                self.trim()

    def trim(self):
        while self._ticks and self._ticks[0] <= time() - 1.0:
            tick_second = int(self._ticks.pop(0))
            tick_count = 1
            while self._ticks and int(self._ticks[0]) == tick_second:
                self._ticks.pop(0)
                tick_count += 1
            self._iadd(tick_count)
        RawSampler.trim(self)

    def _clear(self):
        RawSampler._clear(self)
        self._ticks = []

################################################################################

if __name__ == "__main__":

    print("self-testing module samplers.py:")

    from time import sleep

    ###################################

    def is_close(a, b):
        return abs(a - b) < 0.01

    ###################################

    rs = RawSampler(3.0)

    assert rs.count == 0 and rs._sum == 0 and rs.avg == 0.0 and \
           rs.min == 2**63-1 and rs.max == -2**63 and is_close(rs.dev, 0.0)

    rs += 1
    assert rs.count == 1 and rs._sum == 1 and rs.avg == 1.0 and \
           rs.min == 1 and rs.max == 1 and is_close(rs.dev, 0.0)

    sleep(1.2)
    rs += 3
    assert rs.count == 2 and rs._sum == 4 and rs.avg == 2.0 and \
           rs.min == 1 and rs.max == 3 and is_close(rs.dev, 1.0)

    sleep(1.2)
    rs += 0
    assert rs.count == 3 and rs._sum == 4 and rs.avg == 4/3 and \
           rs.min == 0 and rs.max == 3 and is_close(rs.dev, 1.247)

    sleep(1.2)
    rs += 2
    assert rs.count == 3 and rs._sum == 5 and rs.avg == 5/3 and \
           rs.min == 0 and rs.max == 3 and is_close(rs.dev, 1.247)

    sleep(1.2)
    rs += 1
    assert rs.count == 3 and rs._sum == 3 and rs.avg == 1.0 and \
           rs.min == 0 and rs.max == 2 and is_close(rs.dev, 0.816)

    sleep(1.2)
    rs += 4
    assert rs.count == 3 and rs._sum == 7 and rs.avg == 7/3 and \
           rs.min == 1 and rs.max == 4 and is_close(rs.dev, 1.247)

    sleep(1.2)
    assert rs.count == 2 and rs._sum == 5 and rs.avg == 2.5 and \
           rs.min == 1 and rs.max == 4 and is_close(rs.dev, 1.5)

    sleep(1.2)
    assert rs.count == 1 and rs._sum == 4 and rs.avg == 4.0 and \
           rs.min == 4 and rs.max == 4 and is_close(rs.dev, 0.0)

    sleep(1.2)
    assert rs.count == 0 and rs._sum == 0 and rs.avg == 0.0 and \
           rs.min == 2**63-1 and rs.max == -2**63 and is_close(rs.dev, 0.0)

    ###################################

    rs = RawSampler(0.0)

    assert rs.count == 0 and rs._sum == 0 and rs.avg == 0.0 and \
           rs.min == 2**63-1 and rs.max == -2**63 and is_close(rs.dev, 0.0)

    rs += 1

    assert rs.count == 0 and rs._sum == 0 and rs.avg == 0.0 and \
           rs.min == 2**63-1 and rs.max == -2**63 and is_close(rs.dev, 0.0)

    ###################################

    rs = RateSampler(10.0)

    for i in range(50):
        sleep(0.01)
        rs.tick()

    assert rs.count == 0 and rs._sum == 0 and rs.avg == 0.0 and \
           rs.min == 2**63-1 and rs.max == -2**63 and is_close(rs.dev, 0.0) and \
           len(rs._ticks) == 50

    for i in range(100):
        sleep(0.01)
        rs.tick()

    sleep(1.1)

    assert rs.count in (2, 3) and rs._sum == 150

    ###################################

    print("ok")

################################################################################
# EOF
