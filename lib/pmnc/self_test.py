#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module implements testing facility for Pythomnic3k application modules.
# Whenever an application module containing
#
# if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()
#
# runs, this module manages to copy all the files of the current cage to a
# temporary directory, start the copy and invoke the tested module's method
# self_test. Application module code can tell a self-test run from a regular
# run by value of pmnc.request.self_test attribute. If not None, this attribute
# contains the name of the module being tested.
#
# There is an important difference in configuration files behaviour, which
# can be easily observed by examining any configuration file. During a test
# run access to a configuration file requires that the file contains a separate
# section with overridden configuration settings, for example:
#
# config = \
# {
# "server_address": ("real_server", 1234),
# "encoding": "win1251",
# "timeout": 10.0,
# }
#
# self_test_config = \ <- without this section self-test access will fail
# {
# "server_address": ("test_server", 1234), <- only some settings are overriden
# }
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "self_test", "active_interface" ]

###############################################################################

import sys; from sys import stdout
import os; from os import path as os_path, mkdir, listdir, rmdir, remove, chmod
import tempfile; from tempfile import mkdtemp
import shutil; from shutil import copyfile, copytree, rmtree
import threading; from threading import Lock, current_thread
import time; from time import time, localtime, strftime, sleep
import stat; from stat import S_IWRITE

import os; import sys
main_module_dir = os_path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
main_module_name = os_path.basename(sys.modules["__main__"].__file__)
assert main_module_name.lower().endswith(".py")
main_module_name = main_module_name[:-3]

if __name__ == "__main__": # add pythomnic/lib to sys.path
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import exc_string; from exc_string import exc_string
import pmnc.module_loader; from pmnc.module_loader import ModuleLoader
import pmnc.request; from pmnc.request import Request, fake_request
import typecheck; from typecheck import by_regex, optional, tuple_of

###############################################################################

node_name = "self_test"
cage_name = os_path.basename(main_module_dir)

###############################################################################

def _non_recursive_copy(source_dir, target_dir, mask):

    if not os_path.isdir(source_dir):
        return

    mkdir(target_dir)
    matching_file = by_regex("^{0:s}$".format(mask))

    for f in listdir(source_dir):
        if matching_file(f.lower()):
            sf = os_path.join(source_dir, f)
            tf = os_path.join(target_dir, f)
            if os_path.isfile(sf):
                copyfile(sf, tf)

###############################################################################

def _cage_files_copy(source_cage_dir, target_cage_dir):

    source_dir = source_cage_dir
    target_dir = target_cage_dir
    _non_recursive_copy(source_cage_dir, target_cage_dir, "[A-Za-z0-9_-]+\\.py")

    source_dir = os_path.join(source_cage_dir, "ssl_keys")
    target_dir = os_path.join(target_cage_dir, "ssl_keys")
    _non_recursive_copy(source_dir, target_dir, "[A-Za-z0-9_-]+\\.pem")

###############################################################################

def _create_temp_cage_copy(*, required_dirs):

    test_cages_dir = mkdtemp()

    real_cage_dir = main_module_dir
    test_cage_dir = os_path.join(test_cages_dir, cage_name)
    _cage_files_copy(real_cage_dir, test_cage_dir)

    for required_dir in required_dirs:
        real_required_dir = os_path.join(real_cage_dir, required_dir)
        test_required_dir = os_path.join(test_cage_dir, required_dir)
        copytree(real_required_dir, test_required_dir)

    if cage_name != ".shared":

        real_cage_dir = os_path.normpath(os_path.join(real_cage_dir, "..", ".shared"))
        test_cage_dir = os_path.join(test_cages_dir, ".shared")
        _cage_files_copy(real_cage_dir, test_cage_dir)

    return test_cages_dir

###############################################################################

def _remove_temp_cage_copy(test_cages_dir):

    sleep(3.0) # this allows dust to settle

    def retry_remove(func, path, exc): # this allows to remove a read-only file
        try:
            chmod(path, S_IWRITE)
            func(path)
        except:
            pass

    for i in range(3):
        try:
            if os_path.isdir(test_cages_dir):
                rmtree(test_cages_dir, onerror = retry_remove)
        except:
            sleep(1.0)
    else:
        if os_path.isdir(test_cages_dir):
            rmtree(test_cages_dir, onerror = retry_remove)

###############################################################################

log_priorities = { 1: "ERR", 2: "MSG", 3: "WRN", 4: "LOG", 5: "INF", 6: "DBG" }
log_lock = Lock()

def _log(s, *, priority = 2):
    t = time(); lt = localtime(t)
    c = priority < 2 and "*" or " " # errors are marked with *ERR*
    s = "{0:s}.{1:02d}{2:s}{3:s}{4:s}[{5:s}] {6}".\
        format(strftime("%H:%M:%S", lt), int(t * 100) % 100, c,
               log_priorities.get(priority, "???"), c, current_thread().name, s)
    with log_lock:
        print(s.encode(stdout.encoding, "replace").decode(stdout.encoding))

###############################################################################

def _start_pmnc(test_cages_dir):

    test_cage_dir = os_path.join(test_cages_dir, cage_name)

    Request._self_test = main_module_name
    fake_request()

    pmnc = ModuleLoader(node_name, cage_name, test_cage_dir, _log, 6)
    try:
        pmnc.startup.start()
    except:
        pmnc.startup.stop()
        raise
    else:
        return pmnc

###############################################################################

def _stop_pmnc(pmnc):

    pmnc.startup.stop()

###############################################################################

def run(*, required_dirs: optional(tuple_of(str)) = ()):

    current_thread().name = "self_test"

    _log("***** STARTING SELF-TEST FOR MODULE {0:s} *****".format(main_module_name.upper()))

    test_cages_dir = _create_temp_cage_copy(required_dirs = required_dirs)
    try:
        pmnc = _start_pmnc(test_cages_dir)
        try:
            try:
                current_thread()._pmnc = pmnc # to be used in active_interface
                assert pmnc.request.self_test == main_module_name
                pmnc.__getattr__(main_module_name).self_test()
            except:
                _log("***** FAILURE: {1:s}".format(main_module_name, exc_string()))
            else:
                _log("***** SUCCESS, BUT EXAMINE THE LOG FOR UNEXPECTED ERRORS *****")
        finally:
            _stop_pmnc(pmnc)
    finally:
        _remove_temp_cage_copy(test_cages_dir)

################################################################################
# this utility class simplifies starting up and stopping interfaces in self-tests:
# with active_interface("email_1", extra = "config") as ifc:
#     ... do stuff ...
# because of the way reference to pmnc is passed through current_thread()._pmnc,
# it can only be used in the main thread which is executing the self-test

class active_interface:

    def __init__(self, name, **config):
        self._name, self._config = name, config

    def __enter__(self):
        pmnc = current_thread()._pmnc
        self._ifc = pmnc.interface.create(self._name, **self._config)
        self._ifc.start()
        pmnc.interfaces.set_fake_interface(self._name, self._ifc)
        return self._ifc

    def __exit__(self, t, v, tb):
        pmnc = current_thread()._pmnc
        pmnc.interfaces.delete_fake_interface(self._name)
        try:
            self._ifc.cease()
        finally:
            self._ifc.stop()

################################################################################
# EOF