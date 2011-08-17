#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module is a dispenser of named locks shared across modules.
# A module can get itself a shared lock using
#
# lock = pmnc.shared_locks.get("lock_name")
#
# Pythomnic3k project
# (c) 2005-2009, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "get" ]
__reloadable__ = False

################################################################################

import threading; from threading import Lock

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import shared_lock; from shared_lock import SharedLock

###############################################################################

# module-level state => not reloadable

_shared_locks = {}
_shared_locks_lock = Lock()

###############################################################################

def get(name: str) -> SharedLock:

    with _shared_locks_lock:
        lock = _shared_locks.get(name)
        if lock is None:
            lock = SharedLock(name)
            _shared_locks[name] = lock

    return lock

###############################################################################

def self_test():

    def test_get():

        s1 = pmnc.shared_locks.get("foo")
        assert pmnc.shared_locks.get("foo") is s1
        assert s1.name == "foo"

        assert _shared_locks["foo"] is s1

        s2 = pmnc.shared_locks.get("bar")
        assert s2 is not s1
        assert s2.name == "bar"

        assert _shared_locks["bar"] is s2

        s1.acquire()
        try:
            pass
        finally:
            s1.release()

        assert pmnc.request.acquire_shared(s1)
        try:
            pass
        finally:
            s1.release_shared()

        pmnc.request.acquire(s2)
        try:
            pass
        finally:
            s2.release()

    test_get()

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF