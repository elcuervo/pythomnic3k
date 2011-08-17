#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# Cage performance information, such as CPU times, working set size
# and somewhat unrelated free disk space.
#
# get_free_disk_space(path)
# returns free disk space in integer megabytes
#
# get_working_set_size()
# returns current process'es working set size in integer megabytes
#
# get_cpu_times()
# returns 2-tuple (user time, kernel time) consumed by this process in float seconds
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "get_free_disk_space", "get_working_set_size", "get_cpu_times" ]

###############################################################################

import os; from os import path as os_path, getcwd
import sys; from sys import modules as sys_modules, path as sys_path

try:
    from os import statvfs
    def get_free_disk_space(path):
        vfs = statvfs(os_path.abspath(path))
        return vfs.f_frsize * vfs.f_bavail // 1048576 # note that f_frsize is used, not f_bsize
except ImportError:
    try:
        import win32file
        def get_free_disk_space(path):
            spc, bps, fc, tc = win32file.GetDiskFreeSpace(os_path.splitdrive(os_path.abspath(path))[0])
            return spc * bps * fc // 1048576
    except ImportError:
        def get_free_disk_space(path):
            return None

###############################################################################

try:
    original_sys_path = sys_path[:] # if a self-test of some module from .shared is running,
    if "__main__" in sys_modules:   # current directory contains conflicting resource.py
        main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or getcwd()
        if os_path.isfile(os_path.join(main_module_dir, "resource.py")):
            sys_path.remove(main_module_dir)
    try:
        import resource
    finally:
        sys_path = original_sys_path
    try:
        from resource import getrusage, RUSAGE_SELF
    finally:
        del sys_modules["resource"] # remove the reference to standard resource.py
    def get_working_set_size():
        rusage = getrusage(RUSAGE_SELF)
        return rusage.ru_maxrss // 1024
    def get_cpu_times():
        rusage = getrusage(RUSAGE_SELF)
        return rusage.ru_utime, rusage.ru_stime
except ImportError:
    try:
        import win32process; from win32process import GetProcessMemoryInfo, GetProcessTimes
        import win32con; from win32con import PROCESS_QUERY_INFORMATION, PROCESS_VM_READ
        import win32api; from win32api import OpenProcess, GetCurrentProcessId
        def get_working_set_size():
            return int(GetProcessMemoryInfo(OpenProcess(PROCESS_QUERY_INFORMATION or PROCESS_VM_READ, 0,
                                                        GetCurrentProcessId()))["WorkingSetSize"] / 1048576.0)
        def get_cpu_times():
            process_times = GetProcessTimes(OpenProcess(PROCESS_QUERY_INFORMATION, 0,
                                                        GetCurrentProcessId()))
            return process_times["UserTime"] / 10000000.0, process_times["KernelTime"] / 10000000.0
    except ImportError:
        def get_working_set_size():
            return None
        def get_cpu_times():
            return None

################################################################################

if __name__ == "__main__":

    print("self-testing module perf_info.py:")

    from shutil import rmtree
    from tempfile import mkdtemp
    from socket import socket, AF_INET, SOCK_DGRAM
    from select import select
    from time import time

    ###################################

    def test_get_free_disk_space():

        print("get_free_disk_space: ", end = "")

        td = mkdtemp()
        tf = os_path.join(td, "zero")

        fs1 = get_free_disk_space(td)
        assert fs1 >= 0

        with open(tf, "wb") as f:
            f.write(b"\x00" * 11534336)

        fs2 = get_free_disk_space(td)
        assert 10 <= fs1 - fs2 <= 12

        rmtree(td)

        print("ok")

    test_get_free_disk_space()

    ###################################

    def test_get_working_set_size():

        print("get_working_set_size: ", end = "")

        wss1 = get_working_set_size()
        s = b"\x00" * 1048576 * wss1 * 2
        wss2 = get_working_set_size()

        assert wss2 >= wss1 * 2

        print("ok")

    test_get_working_set_size()

    ###################################

    def test_get_cpu_times():

        print("get_cpu_times: ", end = "")

        ut1, kt1 = get_cpu_times()

        start = time()
        while time() - start < 3.0:
            for i in range(10000000):
                pass

        ut2, kt2 = get_cpu_times()

        assert ut2 - ut1 >= 2.5 and kt2 - kt1 <= 0.5

        ss = [ socket(AF_INET, SOCK_DGRAM) ]

        start = time()
        while time() - start < 3.0:
            for i in range(1000):
                select(ss, ss, ss, 0.0)

        ut3, kt3 = get_cpu_times()

        assert ut3 - ut2 <= 2.5 and kt3 - kt2 >= 0.5

        print("ok")

    test_get_cpu_times()

    ###################################

    print("all ok")

################################################################################
# EOF