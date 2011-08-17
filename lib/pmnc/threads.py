#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module implements two descendants of a Thread class, one - HeavyThread
# is a long-living persistent thread which can be started and stopped once
# and is typically used in interfaces for long-living processing threads,
# two - LightThread is a daemon thread to be used in thread pools. Both kinds
# of threads perform certain initialization, such as CoInitialize under Windows
# before passing control to the target method.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "HeavyThread", "LightThread" ]

################################################################################

import threading; from threading import Event, Thread, current_thread
try:
    import pythoncom; from pythoncom import CoInitializeEx, CoUninitialize, COINIT_MULTITHREADED
except:
    win32_com = False
else:
    win32_com = True

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import typecheck, optional
import pmnc.request; from pmnc.request import InfiniteRequest

################################################################################

if win32_com:
    class PmncThread(Thread):
        def run(self, *args, **kwargs):
            CoInitializeEx(COINIT_MULTITHREADED)
            try:
                return Thread.run(self, *args, **kwargs)
            finally:
                CoUninitialize()
else:
    PmncThread = Thread

################################################################################

class HeavyThread(PmncThread):

    def __init__(self, *args, **kwargs):
        PmncThread.__init__(self, *args, **kwargs)
        self.__stop = Event()

    def run(self):
        current_thread()._request = InfiniteRequest() # has an infinite request attached
        PmncThread.run(self)

    def stopped(self, timeout = None):
        if timeout is not None:
            self.__stop.wait(timeout) # this may spend waiting slightly less, but it's ok
        return self.__stop.is_set()

    def stop(self):
        self.__stop.set()
        if current_thread() is not self:
            self.join()

################################################################################

class LightThread(PmncThread):

    def __init__(self, *args, **kwargs):
        PmncThread.__init__(self, *args, **kwargs)
        self.daemon = True

################################################################################

if __name__ == "__main__":

    print("self-testing module threads.py:")

    from time import sleep

    def ht_proc():
        params = current_thread()._request.parameters
        params["count"] = 0
        while not current_thread().stopped(0.1):
            params["count"] += 1
            if current_thread().stopped():
                break

    ht = HeavyThread(target = ht_proc)
    ht.start()
    sleep(2.0)
    ht.stop()
    assert 15 < ht._request.parameters["count"] < 25

    def lt_proc():
        if win32_com:
            from win32com import client as com_client
            com_client.Dispatch("Msxml2.DOMDocument")

    lt = LightThread(target = lt_proc)
    lt.start()
    sleep(2.0)

    # by not stopping the LightThread instance I test its daemonness

    print("ok")

################################################################################
# EOF
