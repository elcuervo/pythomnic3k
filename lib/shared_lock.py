#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# Shared lock (aka reader-writer lock) with timeouts and FIFO ordering for writers.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "SharedLock", "SharedLockWriterPriority" ]

################################################################################

import threading
import exc_string
import random

################################################################################

class SharedLock:

    def __init__(self, name = None, log = None, debug = False):

        self._name = name or "SharedLock_{0:04X}".format(id(self) % 65521)
        self.__log, self._debug = log, debug

        self._ex_thread, self._ex_depth, self._pending_ex_threads  = None, 0, []
        self._sh_threads, self._pending_sh_threads = {}, []
        self._lock, self._pooled_events = threading.Lock(), []

    name = property(lambda self: self._name)

    ################################### utility log function

    def _log(self, s):
        self.__log("{0:s} {1:s} in {2:s}".format(s, self._dump(),
                                                 exc_string.trace_string(skip = 1)))

    ################################### lock state debug dump

    def _dump(self):

        ex_thread = self._ex_thread and \
                    "{0:s}:{1:d}".format(self._ex_thread.name, self._ex_depth) or "-"

        pending_ex_threads = ", ".join("{0:s}:{1:d}".format(ex_thread.name, sh_depth)
                                       for ex_thread, ex_event, sh_depth in self._pending_ex_threads)
        if pending_ex_threads:
            pending_ex_threads = " <= {0:s}".format(pending_ex_threads)

        sh_threads = ", ".join(sorted("{0:s}:{1:d}".format(sh_thread.name, sh_depth)
                                      for sh_thread, sh_depth in self._sh_threads.items())) or "-"

        pending_sh_threads = ", ".join("{0:s}:{1:d}".format(sh_thread.name, sh_depth)
                                       for sh_thread, sh_event, sh_depth in self._pending_sh_threads)
        if pending_sh_threads:
            pending_sh_threads = " <= {0:s}".format(pending_sh_threads)

        return "{0:s}(ex({1:s}{2:s})sh({3:s}{4:s}))".\
               format(self._name, ex_thread, pending_ex_threads, sh_threads, pending_sh_threads)

    ################################### lock invariant

    def _invariant(self): # invariant checks slow down the lock a lot (~3 times)

        # if some thread has exclusive access, no threads can have
        # shared access and vice versa, except for when it is the same
        # single thread

        if self._ex_thread and self._sh_threads and \
           list(self._sh_threads.keys()) != [self._ex_thread]:
            return 1

        # if no thread is holding the lock, no thread should be pending on it

        if not self._ex_thread and not self._sh_threads and \
           (self._pending_ex_threads or self._pending_sh_threads):
            return 2

        # no thread can be holding a lock zero times and vice versa

        if (self._ex_thread and self._ex_depth <= 0) or \
           (not self._ex_thread and self._ex_depth > 0):
            return 3

        if list(filter(lambda sh_depth: sh_depth <= 0,
                       self._sh_threads.values())):
            return 4

        # if no thread has exclusive access nor pending for it,
        # there should be no threads pending for shared access

        if not self._ex_thread and not self._pending_ex_threads and \
           self._pending_sh_threads:
            return 5

        # if no thread has exclusive access and no threads have shared
        # access, no thread should be pending for exclusive access

        if not self._ex_thread and not self._sh_threads and \
           self._pending_ex_threads:
            return 6

        # a thread can be pending on a lock only once, either for shared
        # or for exclusive access

        pending_threads = list(map(lambda t: t[0], self._pending_sh_threads)) + \
                          list(map(lambda t: t[0], self._pending_ex_threads))

        pending_threads.sort(key = lambda t: id(t))

        for i in range(len(pending_threads) - 1):
            if pending_threads[i] is pending_threads[i+1]:
                return 7

        # everything seems to be fine

        return 0

    ###################################

    def _check_invariant(self):
        i = self._invariant()
        assert i == 0, "invariant {0:d} failed for {1:s}".format(i, self._dump())

    ################################### sleep/wakeup event pool

    def _pick_event(self):
        if len(self._pooled_events):
            event = self._pooled_events.pop(0)
            assert not event.is_set()
            return event
        else:
            return threading.Event()

    def _unpick_event(self, event):
        event.clear()
        self._pooled_events.append(event)

    ################################### determine which pending thread(s) to wake if any

    def _wake_policy(self):

        pending_ex = len(self._pending_ex_threads)
        pending_sh = len(self._pending_sh_threads)

        if self._sh_threads:
            return False, not pending_ex
        elif pending_ex and pending_sh:
            wake_ex = random.randint(0, pending_sh) > 0
            return wake_ex, not wake_ex
        else:
            return pending_ex, pending_sh

    ################################### wake up threads after lock release

    def _release_threads(self):

        if self._ex_thread: # if a lock has exclusive owner, no other thread can be released
            return

        wake_ex, wake_sh = self._wake_policy()

        if wake_ex: # wake up the first thread pending for exclusive access

            self._ex_thread, ex_event, sh_depth = self._pending_ex_threads.pop(0)
            self._ex_depth = 1
            if sh_depth > 0: # restore thread's shared locks
                self._sh_threads[self._ex_thread] = sh_depth
            ex_event.set()

        elif wake_sh: # wake up all threads pending for shared access

            for sh_thread, sh_event, sh_depth in self._pending_sh_threads:
                self._sh_threads[sh_thread] = sh_depth
                sh_event.set()
            del self._pending_sh_threads[:]

    ################################### sleep until the lock is acquired or timeout elapses

    def _acquire_event(self, event, timeout = None):

        current_thread = threading.current_thread()

        if timeout is None:
            event.wait()
        else:                   # this inherits Event's behaviour and may spend
            event.wait(timeout) # less time waiting than requested, see issue9892

        with self._lock:

            result = event.is_set()

            reacquire_shared = False

            if not result: # current thread has failed to acquire the lock

                # if the lock has not been acquired, the thread must be removed
                # from the pending list it's currently on

                # see if current thread failed to acquire shared access

                for i, (sh_thread, sh_event, sh_depth) in enumerate(self._pending_sh_threads):
                    if sh_thread is current_thread and sh_event is event:
                        assert sh_depth == 1
                        del self._pending_sh_threads[i]
                        break

                else: # otherwise see whether it was going for exclusive access

                    for i, (ex_thread, ex_event, sh_depth) in enumerate(self._pending_ex_threads):
                        if ex_thread is current_thread and ex_event is event:
                            del self._pending_ex_threads[i]
                            if sh_depth > 0: # current thread had shared access before it went for exclusive
                                if not self._ex_thread:
                                    self._sh_threads[current_thread] = sh_depth
                                else:
                                    self._pending_sh_threads.append((current_thread, ex_event, sh_depth))
                                    reacquire_shared = True
                            break

                    else: # this should not normally happen

                        assert False

                # if a thread has failed to acquire a lock, it's identical as if it had
                # it and then released, therefore other threads should be released now

                self._release_threads()

            if not reacquire_shared:
                self._unpick_event(event)

            if self._debug: self._check_invariant()
            if self.__log:
                if result:
                    self._log("acquired")
                else:
                    self._log("timed out in {0:.01f} second(s) waiting for".format(timeout))
                    if reacquire_shared:
                        self._log("acquiring previously owned shared access for")

        if reacquire_shared:
            assert self._acquire_event(event)
            return False

        return result

    ################################### exclusive acquire

    def acquire(self, timeout = None):
        """
        Attempts to acquire the lock exclusively within the optional timeout.
        If the timeout is not specified, waits for the lock infinitely.
        Returns True if the lock has been acquired, False otherwise.
        """

        current_thread = threading.current_thread()

        with self._lock:

            if self.__log: self._log("acquiring exclusive")
            if self._debug: self._check_invariant()

            # if this thread already has exclusive access, the count is incremented

            if current_thread is self._ex_thread:

                self._ex_depth += 1

                if self._debug: self._check_invariant()
                if self.__log: self._log("acquired exclusive")

                return True

            # if this thread already has shared access, things can get complicated

            elif current_thread in self._sh_threads:

                # if this thread is the only one accessing the lock, it gets exclusive access

                if not self._pending_sh_threads and not self._pending_ex_threads and \
                   list(self._sh_threads.keys()) == [current_thread]:

                    self._ex_thread = current_thread
                    self._ex_depth = 1

                    if self._debug: self._check_invariant()
                    if self.__log: self._log("acquired exclusive")

                    return True

                # otherwise this thread gives up its shared access in hope for exclusive access

                sh_depth = self._sh_threads.pop(current_thread)

                event = self._pick_event()
                self._pending_ex_threads.append((current_thread, event, sh_depth))

                self._release_threads()

            # this thread acquires exclusive access when no other thread has
            # neither exclusive not shared access

            elif not self._ex_thread and not self._sh_threads:

                self._ex_thread = current_thread
                self._ex_depth = 1

                if self._debug: self._check_invariant()
                if self.__log: self._log("acquired exclusive")

                return True

            # otherwise the thread registers itself as a pending owner with no
            # prior record of holding shared locks

            else:

                event = self._pick_event()
                self._pending_ex_threads.append((current_thread, event, 0))

            if self._debug: self._check_invariant()
            if self.__log: self._log("waiting for exclusive")

        return self._acquire_event(event, timeout) # the thread waits for a lock release

    ################################### shared acquire

    def acquire_shared(self, timeout = None):
        """
        Attempts to acquire the lock in shared mode within the optional
        timeout. If the timeout is not specified, waits for the lock
        infinitely. Returns True if the lock has been acquired, False
        otherwise.
        """

        current_thread = threading.current_thread()

        with self._lock:

            if self.__log: self._log("acquiring shared")
            if self._debug: self._check_invariant()

            # if this thread already has shared access, the access count is incremented

            if current_thread in self._sh_threads:

                self._sh_threads[current_thread] += 1

                if self._debug: self._check_invariant()
                if self.__log: self._log("acquired shared")

                return True

            # if this thread already has exclusive access, it gets shared access easily

            elif current_thread is self._ex_thread:

                if current_thread in self._sh_threads:
                    self._sh_threads[current_thread] += 1
                else:
                    self._sh_threads[current_thread] = 1

                if self._debug: self._check_invariant()
                if self.__log: self._log("acquired shared")

                return True

            # if no other threads have exclusive access nor pending for it,
            # this thread gets shared access

            elif not self._ex_thread and not self._pending_ex_threads:

                self._sh_threads[current_thread] = 1

                if self._debug: self._check_invariant()
                if self.__log: self._log("acquired shared")

                return True

            # otherwise this thread registers itself as pending for shared access

            event = self._pick_event()
            self._pending_sh_threads.append((current_thread, event, 1))

            if self._debug: self._check_invariant()
            if self.__log: self._log("waiting for shared")

        return self._acquire_event(event, timeout) # and waits for a lock release

    ################################### exclusive release

    def release(self):
        """
        Releases the lock previously locked by a call to acquire().
        """

        current_thread = threading.current_thread()

        with self._lock:

            if self.__log: self._log("releasing exclusive")
            if self._debug: self._check_invariant()

            if current_thread is not self._ex_thread:
                assert False, "thread {0:s} has not acquired the lock".format(
                              current_thread.name)

            # the thread gives up its exclusive access

            self._ex_depth -= 1
            if self._ex_depth > 0:

                if self._debug: self._check_invariant()
                if self.__log: self._log("released exclusive")

                return

            self._ex_thread = None
            self._release_threads()

            if self._debug: self._check_invariant()
            if self.__log: self._log("released exclusive")

    ################################### shared release

    def release_shared(self):
        """
        Releases the lock previously locked by a call to acquire_shared().
        """

        current_thread = threading.current_thread()

        with self._lock:

            if self.__log: self._log("releasing shared")
            if self._debug: self._check_invariant()

            if current_thread not in self._sh_threads:
                assert False, "thread {0:s} has not acquired the lock".format(
                              current_thread.name)

            # the thread gives up its shared lock

            self._sh_threads[current_thread] -= 1
            if self._sh_threads[current_thread] > 0:

                if self._debug: self._check_invariant()
                if self.__log: self._log("released shared")

                return

            del self._sh_threads[current_thread]
            self._release_threads()

            if self._debug: self._check_invariant()
            if self.__log: self._log("released shared")

################################################################################
# this subclass overrides the wake up decision method to favor threads
# pending for exclusive access, this is useful if exclusive access
# is infrequent but rather important to acquire sooner than later

class SharedLockWriterPriority(SharedLock):

    def _wake_policy(self):

        pending_ex = len(self._pending_ex_threads)
        pending_sh = len(self._pending_sh_threads)

        if self._sh_threads:
            return False, not pending_ex
        elif pending_ex:
            return True, False
        else:
            return False, pending_sh

################################################################################

if __name__ == "__main__":

    print("self-testing module shared_lock.py:")

    from sys import setcheckinterval
    from time import sleep, time, strftime

    from expected import expected

    setcheckinterval(10) # this makes threads behaviour more uniform

    log_lock = threading.Lock()
    def log(s):
        with log_lock:
            print("{0:s}.{1:02d} {2:<8s} {3:s}".format(strftime("%H:%M:%S"), int((time() * 100) % 100),
                                                       threading.current_thread().name, s))

    def deadlocks(f, t):
        th = threading.Thread(target = f, name = "ThreadName")
        th.daemon = 1
        th.start()
        th.join(t)
        return th.is_alive()

    def threads(n, *f):
        start = time()
        evt = threading.Event()
        ths = [ threading.Thread(target = f[i % len(f)], args = (evt, ),
                                 name = f[i % len(f)].__name__ + "ABCDEFGHIJKLMNOPQRSTUVWXYZ"[i])
                for i in range(n) ]
        for i, th in enumerate(ths):
            th.daemon = 1,
            th.start()
        evt.set()
        for th in ths:
            th.join()
        return time() - start

    # simple test

    print("simple test: ", end = "")

    threading.current_thread().name = "MainThread"

    lck = SharedLock("TestLock", None, True)
    assert lck._dump() == "TestLock(ex(-)sh(-))", lck._dump()

    assert lck.acquire()
    assert lck._dump() == "TestLock(ex(MainThread:1)sh(-))", lck._dump()
    lck.release()
    assert lck._dump() == "TestLock(ex(-)sh(-))", lck._dump()

    assert lck.acquire_shared()
    assert lck._dump() == "TestLock(ex(-)sh(MainThread:1))", lck._dump()
    lck.release_shared()
    assert lck._dump() == "TestLock(ex(-)sh(-))", lck._dump()

    with expected(AssertionError("thread MainThread has not acquired the lock")):
        lck.release()

    with expected(AssertionError("thread MainThread has not acquired the lock")):
        lck.release_shared()

    # check that a lock can be acquired in no time

    assert lck.acquire(0.0)
    lck.release()

    assert lck.acquire_shared(0.0)
    lck.release_shared()

    print("ok")

    # recursion test

    print("recursive lock test: ", end = "")

    lck = SharedLock("RecLock", None, True)

    assert lck.acquire()
    assert lck.acquire()
    assert lck._dump() == "RecLock(ex(MainThread:2)sh(-))", lck._dump()
    lck.release()
    assert lck._dump() == "RecLock(ex(MainThread:1)sh(-))", lck._dump()
    lck.release()

    assert lck.acquire_shared()
    assert lck.acquire_shared()
    assert lck._dump() == "RecLock(ex(-)sh(MainThread:2))", lck._dump()
    lck.release_shared()
    assert lck._dump() == "RecLock(ex(-)sh(MainThread:1))", lck._dump()
    lck.release_shared()

    print("ok")

    # same thread shared/exclusive upgrade test

    print("same thread shared/exclusive upgrade test: ", end = "")

    lck = SharedLock("UpgLock", None, True)

    def upgrade():

        # ex -> sh <- sh <- ex

        assert lck._dump() == "UpgLock(ex(-)sh(-))", lck._dump()
        assert lck.acquire()
        assert lck._dump() == "UpgLock(ex(ThreadName:1)sh(-))", lck._dump()
        assert lck.acquire_shared()
        assert lck._dump() == "UpgLock(ex(ThreadName:1)sh(ThreadName:1))", lck._dump()
        lck.release_shared()
        assert lck._dump() == "UpgLock(ex(ThreadName:1)sh(-))", lck._dump()
        lck.release()
        assert lck._dump() == "UpgLock(ex(-)sh(-))", lck._dump()

        # ex -> sh <- ex <- sh

        assert lck._dump() == "UpgLock(ex(-)sh(-))", lck._dump()
        assert lck.acquire()
        assert lck._dump() == "UpgLock(ex(ThreadName:1)sh(-))", lck._dump()
        assert lck.acquire_shared()
        assert lck._dump() == "UpgLock(ex(ThreadName:1)sh(ThreadName:1))", lck._dump()
        lck.release()
        assert lck._dump() == "UpgLock(ex(-)sh(ThreadName:1))", lck._dump()
        lck.release_shared()
        assert lck._dump() == "UpgLock(ex(-)sh(-))", lck._dump()

        # sh -> ex <- ex <- sh

        assert lck._dump() == "UpgLock(ex(-)sh(-))", lck._dump()
        assert lck.acquire_shared()
        assert lck._dump() == "UpgLock(ex(-)sh(ThreadName:1))", lck._dump()
        assert lck.acquire()
        assert lck._dump() == "UpgLock(ex(ThreadName:1)sh(ThreadName:1))", lck._dump()
        lck.release()
        assert lck._dump() == "UpgLock(ex(-)sh(ThreadName:1))", lck._dump()
        lck.release_shared()
        assert lck._dump() == "UpgLock(ex(-)sh(-))", lck._dump()

        # sh -> ex <- sh <- ex

        assert lck._dump() == "UpgLock(ex(-)sh(-))", lck._dump()
        assert lck.acquire_shared()
        assert lck._dump() == "UpgLock(ex(-)sh(ThreadName:1))", lck._dump()
        assert lck.acquire()
        assert lck._dump() == "UpgLock(ex(ThreadName:1)sh(ThreadName:1))", lck._dump()
        lck.release_shared()
        assert lck._dump() == "UpgLock(ex(ThreadName:1)sh(-))", lck._dump()
        lck.release()
        assert lck._dump() == "UpgLock(ex(-)sh(-))", lck._dump()

    assert not deadlocks(upgrade, 2.0)

    print("ok")

    # timeout test

    print("timeout test: ", end = "")

    # exclusive/exclusive timeout

    lck = SharedLock("ExExTimeoutLock", None, True)

    def f(evt):
        evt.wait()
        assert lck.acquire()
        assert lck._dump() == "ExExTimeoutLock(ex(fA:1)sh(-))", lck._dump()
        sleep(1.0)
        assert lck._dump() == "ExExTimeoutLock(ex(fA:1 <= gB:0)sh(-))", lck._dump()
        lck.release()

    def g(evt):
        evt.wait()
        sleep(0.5)
        assert lck._dump() == "ExExTimeoutLock(ex(fA:1)sh(-))", lck._dump()
        assert not lck.acquire(0.25)
        assert lck._dump() == "ExExTimeoutLock(ex(fA:1)sh(-))", lck._dump()
        assert lck.acquire(0.5)
        assert lck._dump() == "ExExTimeoutLock(ex(gB:1)sh(-))", lck._dump()
        lck.release()

    threads(2, f, g)

    # shared/shared no timeout

    lck = SharedLock("ShShNoTimeoutLock", None, True)

    def f(evt):
        evt.wait()
        assert lck.acquire_shared()
        assert lck._dump() == "ShShNoTimeoutLock(ex(-)sh(fA:1))", lck._dump()
        sleep(1.0)
        assert lck._dump() == "ShShNoTimeoutLock(ex(-)sh(fA:1))", lck._dump()
        lck.release_shared()

    def g(evt):
        evt.wait()
        sleep(0.5)
        assert lck._dump() == "ShShNoTimeoutLock(ex(-)sh(fA:1))", lck._dump()
        assert lck.acquire_shared(0.25)
        assert lck._dump() == "ShShNoTimeoutLock(ex(-)sh(fA:1, gB:1))", lck._dump()
        lck.release_shared()

    threads(2, f, g)

    # exclusive/shared timeout

    lck = SharedLock("ExShTimeoutLock", None, True)

    def f(evt):
        evt.wait()
        assert lck.acquire()
        assert lck._dump() == "ExShTimeoutLock(ex(fA:1)sh(-))", lck._dump()
        sleep(1.0)
        assert lck._dump() == "ExShTimeoutLock(ex(fA:1)sh(- <= gB:1))", lck._dump()
        lck.release()

    def g(evt):
        evt.wait()
        sleep(0.5)
        assert lck._dump() == "ExShTimeoutLock(ex(fA:1)sh(-))", lck._dump()
        assert not lck.acquire_shared(0.25)
        assert lck._dump() == "ExShTimeoutLock(ex(fA:1)sh(-))", lck._dump()
        assert lck.acquire_shared(0.5)
        assert lck._dump() == "ExShTimeoutLock(ex(-)sh(gB:1))", lck._dump()
        lck.release_shared()

    threads(2, f, g)

    # shared/exclusive timeout

    lck = SharedLock("ShExTimeoutLock", None, True)

    def f(evt):
        evt.wait()
        assert lck.acquire_shared()
        assert lck._dump() == "ShExTimeoutLock(ex(-)sh(fA:1))", lck._dump()
        sleep(1.0)
        assert lck._dump() == "ShExTimeoutLock(ex(- <= gB:0)sh(fA:1))", lck._dump()
        lck.release_shared()

    def g(evt):
        evt.wait()
        sleep(0.5)
        assert lck._dump() == "ShExTimeoutLock(ex(-)sh(fA:1))", lck._dump()
        assert not lck.acquire(0.25)
        assert lck._dump() == "ShExTimeoutLock(ex(-)sh(fA:1))", lck._dump()
        assert lck.acquire(0.5)
        assert lck._dump() == "ShExTimeoutLock(ex(gB:1)sh(-))", lck._dump()
        lck.release()

    threads(2, f, g)

    # re-acquiring previously owned shared locks after an upgrade timeout

    lck = SharedLock("ShUpgTimeoutLock", None, True)

    def f(evt):
        evt.wait()
        assert lck.acquire_shared()
        assert lck._dump() == "ShUpgTimeoutLock(ex(-)sh(fA:1))", lck._dump()
        sleep(1.0)
        before = time()
        assert not lck.acquire(0.1) # this fails to acquire the lock but
        after = time()
        assert after - before > 2.0 # blocks for way more than 0.1 sec.
        assert lck._dump() == "ShUpgTimeoutLock(ex(-)sh(fA:1))", lck._dump()

    def g(evt):
        evt.wait()
        sleep(0.5)
        assert lck._dump() == "ShUpgTimeoutLock(ex(-)sh(fA:1))", lck._dump()
        assert lck.acquire()
        assert lck._dump() == "ShUpgTimeoutLock(ex(gB:1 <= fA:1)sh(-))", lck._dump()
        sleep(2.1)
        lck.release()

    threads(2, f, g)

    print("ok")

    # zero timeout test

    print("zero timeout test: ", end = "")

    lck = SharedLock("ZeroLock", None, True)

    def f(evt):
        evt.wait()
        assert lck.acquire()
        assert lck._dump() == "ZeroLock(ex(fA:1)sh(-))", lck._dump()
        sleep(1.0)
        lck.release()

    def g(evt):
        evt.wait()
        sleep(0.5)
        assert lck._dump() == "ZeroLock(ex(fA:1)sh(-))", lck._dump()
        assert not lck.acquire(0.0)
        assert lck._dump() == "ZeroLock(ex(fA:1)sh(-))", lck._dump()
        assert not lck.acquire_shared(0.0)
        assert lck._dump() == "ZeroLock(ex(fA:1)sh(-))", lck._dump()
        sleep(1.0)
        assert lck._dump() == "ZeroLock(ex(-)sh(-))", lck._dump()
        assert lck.acquire(0.0)
        assert lck._dump() == "ZeroLock(ex(gB:1)sh(-))", lck._dump()
        lck.release()
        assert lck.acquire_shared(0.0)
        assert lck._dump() == "ZeroLock(ex(-)sh(gB:1))", lck._dump()
        lck.release_shared()

    threads(2, f, g)

    print("ok")

    # different threads shared/exclusive upgrade test

    print("different threads shared/exclusive upgrade test: ", end = "")

    lck = SharedLock("ShExUpgLock", None, True)

    def f(evt):
        evt.wait()
        assert lck._dump() == "ShExUpgLock(ex(-)sh(-))", lck._dump()
        assert lck.acquire_shared()
        assert lck._dump() == "ShExUpgLock(ex(-)sh(fA:1))", lck._dump()
        sleep(3.0)
        lck.release_shared()

    def g(evt):
        evt.wait()
        sleep(1.0)
        assert lck._dump() == "ShExUpgLock(ex(-)sh(fA:1))", lck._dump()
        assert lck.acquire_shared()
        assert lck._dump() == "ShExUpgLock(ex(-)sh(fA:1, gB:1))", lck._dump()
        sleep(3.0)
        assert lck._dump() == "ShExUpgLock(ex(- <= hC:0)sh(gB:1))", lck._dump()
        assert lck.acquire()
        assert lck._dump() == "ShExUpgLock(ex(gB:1)sh(gB:1))", lck._dump()
        lck.release()
        lck.release_shared()

    def h(evt):
        evt.wait()
        sleep(2.0)
        assert lck._dump() == "ShExUpgLock(ex(-)sh(fA:1, gB:1))", lck._dump()
        assert lck.acquire()
        assert lck._dump() == "ShExUpgLock(ex(hC:1 <= gB:1)sh(-))", lck._dump()
        sleep(1.0)
        lck.release()

    threads(3, f, g, h)

    print("ok")

    # different threads exclusive/exclusive deadlock test

    print("different threads exclusive/exclusive deadlock test: ", end = "")

    lck = SharedLock("ExExDeadlockLock", None, True)

    def deadlock(evt):
        lck.acquire()

    assert deadlocks(lambda: threads(2, deadlock), 3.0)

    print("ok")

    # different thread shared/exclusive deadlock test

    print("different threads shared/exclusive deadlock test: ", end = "")

    lck = SharedLock("ShExDeadlockLock", None, True)

    def deadlock1(evt):
        lck.acquire()

    def deadlock2(evt):
        lck.acquire_shared()

    assert deadlocks(lambda: threads(2, deadlock1, deadlock2), 3.0)

    print("ok")

    # different thread shared/shared deadlock test

    print("different threads shared/shared no deadlock test: ", end = "")

    lck = SharedLock("ShShNoDeadlockLock", None, True)

    def deadlock(evt):
        lck.acquire_shared()

    assert not deadlocks(lambda: threads(2, deadlock), 3.0)

    print("ok")

    # cross upgrade test

    print("different threads cross upgrade test: ", end = "")

    lck = SharedLock("CrossUpgLock", None, True)

    def cross(evt):
        lck.acquire_shared()
        sleep(0.5)
        lck.acquire()
        sleep(0.25)
        lck.release()
        sleep(0.5)
        lck.release_shared()

    assert not deadlocks(lambda: threads(2, cross), 3.0)

    print("ok")

    # exclusive interlock + timing test

    print("exclusive interlock + serialized timing test: ", end = "")

    lck = SharedLock("ExTimeLock", None, True)
    val = 0

    def exclusive(evt):
        evt.wait()
        global val
        for i in range(10):
            lck.acquire()
            try:
                assert val == 0
                val += 1
                sleep(0.05 + random.random() * 0.05)
                assert val == 1
                val -= 1
                sleep(0.05 + random.random() * 0.05)
                assert val == 0
            finally:
                lck.release()

    assert threads(4, exclusive) > 0.05 * 2 * 10 * 4

    print("ok")

    # shared non-interlock timing test

    print("shared parallel timing test: ", end = "")

    lck = SharedLock("ShTimeLock", None, True)

    def shared(evt):
        evt.wait()
        for i in range(10):
            lck.acquire_shared()
            try:
                sleep(random.random())
            finally:
                lck.release_shared()

    assert threads(10, shared) < 0.5 * 10 + 5.0

    print("ok")

    # shared/exclusive test

    print("multiple exclusive/shared threads busy loops:")

    ex, sh, start, t = 0, 0, time(), 10.0

    def exclusive(evt):
        global ex
        evt.wait()
        while time() < start + t:
            for j in range(random.randint(0, 100)): pass
            lck.acquire()
            try:
                for j in range(random.randint(0, 100)): pass
                ex += 1
            finally:
                lck.release()

    def shared(evt):
        global sh
        evt.wait()
        while time() < start + t:
            for j in range(random.randint(0, 100)): pass
            lck.acquire_shared()
            try:
                for j in range(random.randint(0, 100)): pass
                sh += 1
            finally:
                lck.release_shared()

    # even distribution

    print("4ex/4sh: ", end = "")

    ex, sh, start, lck = 0, 0, time(), SharedLock("BusyLock/4+4", None, True)
    assert 10.0 <= threads(8, exclusive, exclusive, exclusive, exclusive, shared, shared, shared, shared) <= 20.0
    print("{0:d}/{1:d} or ".format(ex, sh), end = "")

    ex, sh, start, lck = 0, 0, time(), SharedLockWriterPriority("BusyLockWriter/4+4", None, True)
    assert 10.0 <= threads(8, exclusive, exclusive, exclusive, exclusive, shared, shared, shared, shared) <= 20.0
    print("{0:d}/{1:d} (with high ex priority): ".format(ex, sh), end = "")

    print("ok")

    # exclusive starvation

    print("1ex/7sh: ", end = "")

    ex, sh, start, lck = 0, 0, time(), SharedLock("BusyLock/1+7", None, True)
    assert 10.0 <= threads(8, exclusive, shared, shared, shared, shared, shared, shared, shared) <= 20.0
    print("{0:d}/{1:d} or ".format(ex, sh), end = "")

    ex, sh, start, lck = 0, 0, time(), SharedLockWriterPriority("BusyLockWriter/1+7", None, True)
    assert 10.0 <= threads(8, exclusive, shared, shared, shared, shared, shared, shared, shared) <= 20.0
    print("{0:d}/{1:d} (with high ex priority): ".format(ex, sh), end = "")

    print("ok")

    # shared starvation

    print("7ex/1sh: ", end = "")

    ex, sh, start, lck = 0, 0, time(), SharedLock("BusyLock/7+1", None, True)
    assert 10.0 <= threads(8, exclusive, exclusive, exclusive, exclusive, exclusive, exclusive, exclusive, shared) <= 20.0
    print("{0:d}/{1:d} or ".format(ex, sh), end = "")

    ex, sh, start, lck = 0, 0, time(), SharedLockWriterPriority("BusyLockWriter/7+1", None, True)
    assert 10.0 <= threads(8, exclusive, exclusive, exclusive, exclusive, exclusive, exclusive, exclusive, shared) <= 20.0
    print("{0:d}/{1:d} (with high ex priority): ".format(ex, sh), end = "")

    print("ok")

    # heavy threading test

    print("exhaustive threaded test (30 sec): ", end = "")

    lck = SharedLock("ExhLock", None, True)
    start, t = time(), 30.0

    def f(evt):

        global start, t
        evt.wait()

        while time() < start + t:

            sleep(random.random() * 0.1)

            j = random.randint(0, 1)
            if j == 0:
                jack = lck.acquire(*(random.randint(0, 1) == 0 and (random.random(), ) or ()))
            else:
                jack = lck.acquire_shared(*(random.randint(0, 1) == 0 and (random.random(), ) or ()))

            sleep(random.random() * 0.1)

            k = random.randint(0, 1)
            if k == 0:
                kack = lck.acquire(*(random.randint(0, 1) == 0 and (random.random(), ) or ()))
            else:
                kack = lck.acquire_shared(*(random.randint(0, 1) == 0 and (random.random(), ) or ()))

            sleep(random.random() * 0.1)

            l = random.randint(0, 1)
            if l == 0:
                lack = lck.acquire(*(random.randint(0, 1) == 0 and (random.random(), ) or ()))
            else:
                lack = lck.acquire_shared(*(random.randint(0, 1) == 0 and (random.random(), ) or ()))

            sleep(random.random() * 0.1)

            if lack:
                if l == 0:
                    lck.release()
                else:
                    lck.release_shared()

            sleep(random.random() * 0.1)

            if kack:
                if k == 0:
                    lck.release()
                else:
                    lck.release_shared()

            sleep(random.random() * 0.1)

            if jack:
                if j == 0:
                    lck.release()
                else:
                    lck.release_shared()

    threads(16, f, f, f, f, f, f, f, f, f, f, f, f, f, f, f, f)

    print("ok")

    # specific anti-owners scenario (users cooperate by passing the lock
    # to each other to make owner starve to death)

    print("shareds cooperate in attempt to make exclusive starve to death: ", end = "")

    lck, shlck, hold = SharedLock("CoopLock", None, True), threading.Lock(), 0
    evtlock, stop = threading.Event(), threading.Event()

    def user(evt):

        evt.wait()

        try:

            while not stop.is_set():

                lck.acquire_shared()
                try:

                    evtlock.set()

                    shlck.acquire()
                    try:
                        global hold
                        hold += 1
                    finally:
                        shlck.release()

                    sleep(random.random() * 0.4)

                    waited = time()
                    while time() - waited < 3.0:
                        shlck.acquire()
                        try:
                            if hold > 1:
                                hold -= 1
                                break
                        finally:
                            shlck.release()

                    if time() - waited >= 3.0: # but in turn they lock themselves
                        raise Exception("didn't work")

                finally:
                    lck.release_shared()

                sleep(random.random() * 0.1)

        except Exception as e:
            assert str(e) == "didn't work"

    def owner(evt):
        evt.wait()
        evtlock.wait()
        lck.acquire()
        lck.release()
        stop.set()

    assert not deadlocks(lambda: threads(5, owner, user, user, user, user), 10.0)

    print("ok")

    # benchmark

    print("benchmark (10 sec):")

    lck, ii = SharedLock(), 0

    start = time()
    while time() < start + 5.0:
        for i in range(100):
            lck.acquire()
            lck.release()
            ii += 1

    print("{0:d} empty exclusive lock/unlock cycles per second".format(ii // 10))

    lck, ii = SharedLock(), 0

    start = time()
    while time() < start + 5.0:
        for i in range(100):
            lck.acquire_shared()
            lck.release_shared()
            ii += 1

    print("{0:d} empty shared lock/unlock cycles per second".format(ii // 10))

    # all ok

    print("all ok")

################################################################################
# EOF
