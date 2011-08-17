#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module implements a pair or complementary functions:
# popen - for launching a child process (using subprocess, what else)
# fopen - for opening a file (using os.open)
# the reason for this wrapping is that file handles are by default inherited
# by child processes and there is no way to overcome that in Windows, unless
# you give up stdin/stdout redirection. Opening at least some files with explicit
# "no inheritance" flag seems to be the only practical solution.
#
# Pythomnic3k project
# (c) 2005-2009, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "popen", "fopen" ]

################################################################################

import subprocess; from subprocess import Popen, PIPE
import sys; from sys import platform
import threading; from threading import Lock
import os; from os import open

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

################################################################################

_Popen_lock = Lock()

def popen(*args) -> Popen:

    command = [str(arg) for arg in args]

    with _Popen_lock: # just in case, see http://bugs.python.org/issue2320
        if platform == "win32":
            return Popen(command, stdin = PIPE, stdout = PIPE, stderr = PIPE) # win32 doesn't support close_fds with stream redirection
        else:
            return Popen(command, stdin = PIPE, stdout = PIPE, stderr = PIPE, close_fds = True)

################################################################################

if platform == "win32":
    from os import O_NOINHERIT
    def fopen(*args):
        return os.open(args[0], args[1] | O_NOINHERIT, *args[2:])
else:
    fopen = os.open

################################################################################

if __name__ == "__main__":

    print("self-testing module popen.py:")

    from time import time
    from os import close, O_RDONLY, remove
    from tempfile import mkstemp

    ###################################

    def test_popen():

        before = time()
        p = popen("sleep", 2)
        assert p.wait() == 0
        after = time()
        assert after - before > 1.5

        p = popen("cat")
        try:
            p.stdin.write(b"foo\n")
            p.stdin.flush()
            assert p.stdout.readline() == b"foo\n"
        finally:
            p.kill()

    test_popen()

    ###################################

    def test_fopen():

        h, fn = mkstemp()
        close(h)
        h = fopen(fn, O_RDONLY)
        close(h)
        remove(fn)

    test_fopen()

    ###################################

    def test_child():

        h, fn = mkstemp()
        close(h)
        h = fopen(fn, O_RDONLY)
        p = popen("sleep", 2)
        close(h)
        remove(fn) # see if we can remove the file while child process is live
        p.wait()

    test_child()

    ###################################

    print("ok")

################################################################################
# EOF
