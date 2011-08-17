#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################

__all__ = [ "start", "stop", "wait", "exit" ]
__reloadable__ = False

################################################################################

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import threading; from threading import Event

###############################################################################

# module-level state => not reloadable

_stop = Event()

###############################################################################

def start():

    pmnc.state.start()
    pmnc.performance.start()
    pmnc.interfaces.start()

###############################################################################

def wait(timeout: float) -> bool:

    _stop.wait(timeout) # this may spend waiting slightly less, but it's ok
    return _stop.is_set()

###############################################################################

def exit():

    _stop.set()

###############################################################################

def stop():

    pmnc.interfaces.stop()
    pmnc.performance.stop()
    pmnc.state.stop()

###############################################################################
# EOF