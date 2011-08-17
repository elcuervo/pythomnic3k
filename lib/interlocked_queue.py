#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# Interlocked queues (FIFO and prioritized) for sending data between threads.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "InterlockedQueue", "InterlockedPriorityQueue" ]

################################################################################

import threading; from threading import Lock, Event
import heapq; from heapq import heappush, heappop
import time; from time import time

################################################################################

class InterlockedQueue:

    def __init__(self):
        self._queue, self._lock, self._signal = [], Lock(), Event()

    def _push(self, item):
        self._queue.append(item)

    def push(self, item):
        with self._lock:
            self._push(item)
            self._signal.set()

    def _pop(self):
        return self._queue.pop(0)

    def pop(self, timeout = None): # respects wall-time timeout, see issue9892
        start, remain = time(), timeout
        while remain is None or remain >= 0.0:
            self._signal.wait(remain)
            with self._lock:
                if self._signal.is_set():
                    assert len(self._queue) > 0
                    if len(self._queue) == 1:
                        self._signal.clear()
                    return self._pop()
                else:
                    assert len(self._queue) == 0
                    if timeout is not None:
                        if timeout == 0.0: # zero timeout => exactly one attempt
                            return None
                        remain = timeout - (time() - start)
        else:
            return None

    def __len__(self):
        with self._lock:
            return len(self._queue)

################################################################################

class InterlockedPriorityQueue(InterlockedQueue):

    def _push(self, item):
        heappush(self._queue, item)

    def _pop(self):
        return heappop(self._queue)

################################################################################

if __name__ == "__main__":

    print("self-testing module interlocked_queue.py:")

    ###################################

    from threading import Thread, current_thread
    from time import sleep
    from random import random

    ###################################

    def test_queue(cls):

        que = cls()

        que.push(123)
        assert que.pop() == 123
        que.push(456)
        assert que.pop(0.0) == 456
        que.push(789)
        assert que.pop(0.1) == 789
        assert que.pop(0.0) is None
        assert que.pop(0.1) is None

        que.push(123)
        que.push(456)

        assert que.pop(0.5) == 123
        assert que.pop() == 456

        before = time()
        assert que.pop(0.1) is None
        after = time()
        assert after - before >= 0.1

        before = time()
        assert que.pop(0.0) is None
        after = time()
        assert after - before < 0.01

        ###############################

        go = Event()

        def pop(q, t = None):
            go.wait()
            current_thread().result = q.pop(t)

        ###############################

        th1 = Thread(target = pop, args = (que, ))
        th1.start()

        th2 = Thread(target = pop, args = (que, ))
        th2.start()

        go.set()

        que.push(1)
        sleep(0.5)
        assert (th1.is_alive() and not th2.is_alive() and th2.result == 1) or \
               (th2.is_alive() and not th1.is_alive() and th1.result == 1)

        que.push(2)
        sleep(0.5)
        assert not th1.is_alive() and not th2.is_alive()
        assert (th1.result == 2 and th2.result == 1) or \
               (th2.result == 2 and th1.result == 1)

        ###############################

        assert len(que) == 0
        go.clear()

        th0 = Thread(target = pop, args = (que, 0.5))
        th0.start()

        th1 = Thread(target = pop, args = (que, 1.5))
        th1.start()

        th2 = Thread(target = pop, args = (que, 1.5))
        th2.start()

        go.set()

        sleep(1.0)
        assert not th0.is_alive() and th0.result is None

        que.push("A")
        sleep(0.25)
        assert (th1.is_alive() and not th2.is_alive() and th2.result == "A") or \
               (th2.is_alive() and not th1.is_alive() and th1.result == "A")

        sleep(0.5)
        assert not th1.is_alive() and not th2.is_alive()
        assert (th1.result == "A" and th2.result is None) or \
               (th2.result == "A" and th1.result is None)

        ###############################

        lock = Lock()
        pushed = []
        popped = []

        start = time()
        go = Event()

        def th_proc(n):

            go.wait()

            i = 0
            while time() < start + 10.0:

                if random() > 0.5:
                    i += 1
                    x = (n, i)
                    que.push(x)
                    with lock:
                        pushed.append(x)

                if random() > 0.5:
                    x = que.pop(random() / 10)
                    if x is not None:
                        with lock:
                            popped.append(x)

            x = que.pop(random() / 10)
            while x is not None:
                with lock:
                    popped.append(x)
                x = que.pop(random() / 10)

        ths = [ Thread(target = th_proc, args = (i, )) for i in range(10) ]
        for th in ths: th.start()
        go.set()
        for th in ths: th.join()

        assert sorted(pushed) == sorted(popped)

    ###################################

    test_queue(InterlockedQueue)
    test_queue(InterlockedPriorityQueue)

    ###################################

    ilpq = InterlockedPriorityQueue()

    ilpq.push(2)
    ilpq.push(3)
    ilpq.push(1)

    assert len(ilpq) == 3

    assert ilpq.pop() == 1
    assert ilpq.pop() == 2
    assert ilpq.pop() == 3

    assert len(ilpq) == 0

    ###################################

    print("ok")

################################################################################
# EOF
