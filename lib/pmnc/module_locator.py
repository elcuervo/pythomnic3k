#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# ModuleLocator helper class implements the lookup of module files within a cage.
# ModuleLoader uses a single global instance of ModuleLocator per cage.
#
# Pythomnic3k project
# (c) 2005-2009, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "ModuleLocator" ]

###############################################################################

import os; from os import path as os_path, listdir

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import typecheck, by_regex, optional

###############################################################################

class ModuleLocator():

    @typecheck
    def __init__(self, cage_directory: os_path.isdir):
        self._cage_directory = os_path.normpath(cage_directory)
        shared_directory = os_path.normpath(os_path.join(cage_directory, "..", ".shared"))
        self._shared_directory = os_path.isdir(shared_directory) and shared_directory or None

    @staticmethod
    def _listdir(s):
        try:
            return listdir(s)
        except:
            return []

    @typecheck
    def locate(self, module_name: by_regex("^[A-Za-z0-9_-]{1,128}\\.pyc?$")) -> optional(os_path.isfile):
        if module_name in self._listdir(self._cage_directory):
            return os_path.join(self._cage_directory, module_name)
        elif self._shared_directory and module_name in self._listdir(self._shared_directory):
            return os_path.join(self._shared_directory, module_name)
        else:
            return None # not found

###############################################################################

if __name__ == "__main__":

    print("self-testing module module_locator.py: ")

    ###################################

    from tempfile import mkdtemp
    from os import mkdir, rmdir, remove

    from expected import expected
    from typecheck import InputParameterError

    ###################################

    cages_directory = mkdtemp()

    shared_directory = os_path.join(cages_directory, ".shared")
    mkdir(shared_directory)

    cage_directory = os_path.join(cages_directory, "test")
    mkdir(cage_directory)

    ###################################

    ml = ModuleLocator(cage_directory)

    with expected(InputParameterError):
        ModuleLocator(cage_directory + "notthere")

    ###################################

    with expected(InputParameterError):
        ml.locate("foo.txt")

    with expected(InputParameterError):
        ml.locate("foo..py")

    assert ml.locate("foo.py") is None

    dfn = os_path.join(shared_directory, "foo.py")
    df = open(dfn, "w").close()
    assert ml.locate("foo.py") == dfn

    cfn = os_path.join(cage_directory, "foo.py")
    cf = open(cfn, "w").close()
    assert ml.locate("foo.py") == cfn

    remove(dfn)
    assert ml.locate("foo.py") == cfn

    remove(cfn)
    assert ml.locate("foo.py") is None

    ###################################

    rmdir(cage_directory)
    rmdir(shared_directory)
    rmdir(cages_directory)

    assert ml.locate("foo.py") is None

    ###################################

    print("ok")

###############################################################################
# EOF
