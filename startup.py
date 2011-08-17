#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
###############################################################################
#
# This is the main startup module for Pythomnic3k. It starts a separate secondary
# instance of Python with the specified cage in it and waits for it to terminate.
# If the started secondary Python exits with non-zero retcode, it is restarted
# automatically. Similarly, if this primary Python exits, the secondary Python
# shuts down the cage. Therefore, to properly terminate the cage, you should
# kill this primary Python instance using its pid file in /cages/cage/logs/cage.pid
# and wait for the secondary Python instance to shut down.
#
# Usage:
# c:> startup.py [node.]cage
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

import sys; from sys import argv, modules, stdout, path as sys_path, executable as python
import os; from os import path as os_path, getcwd, mkdir, write, fsync, close, getpid
import threading; from threading import Lock, current_thread
import time; from time import time, localtime, strftime, sleep
import platform; from platform import node as node_name
import errno; from errno import EEXIST
try:
    import pythoncom; from pythoncom import PumpMessages
    import win32con; from win32con import WM_QUIT
    import win32api; from win32api import GetCurrentThreadId, PostThreadMessage
except:
    win32_com = False
else:
    win32_com = True

pmnc_dir = os_path.dirname(modules["__main__"].__file__) or getcwd()
cages_dir = os_path.join(pmnc_dir, "cages")

lib_dir = os_path.join(pmnc_dir, "lib")
sys_path.insert(0, lib_dir)

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, optional, one_of, by_regex
import pmnc.module_loader; from pmnc.module_loader import ModuleLoader
import pmnc.request; from pmnc.request import fake_request
import pmnc.popen; from pmnc.popen import popen, fopen
import pmnc.threads; from pmnc.threads import LightThread, HeavyThread

#######################################

if len(argv) == 1:
    raise SystemExit("Pythomnic3k startup script:\r\nc:> startup.py [node.]cage")

#######################################

@typecheck
def primary_startup(node_cage: by_regex("^[A-Za-z0-9_-]{1,32}(\\.[A-Za-z0-9_-]{1,32})?$"),
                    default_log_level: optional(one_of("ERROR", "WARNING", "LOG", "INFO", "DEBUG")) = None):

    if "." in node_cage: # node name is specified explicitly
        node, cage = node_cage.split(".")
    else: # node name is taken from the environment
        cage = node_cage
        node = node_name().split(".")[0]

    cage_dir = os_path.join(cages_dir, cage) # cage directory must exist
    if not os_path.isdir(cage_dir):
        raise Exception("cage directory does not exist")

    logs_dir = os_path.join(cage_dir, "logs") # while logs directory will be created if necessary
    if not os_path.isdir(logs_dir):
        try:
            mkdir(logs_dir)
        except OSError as e:
            if e.errno != EEXIST:
                raise

    # write own pid file, this also serves as a test of the logs directory writability

    with open(os_path.join(logs_dir, "{0:s}.pid".format(cage)), "wb") as f:
        f.write("{0:d}".format(getpid()).encode("ascii"))

    restarting_after_failure = False # performing normal startup by default

    while True: # keep starting secondary startup script until it exits successfully

        def drain_stream(stream):
            try:
                while stream.read(512):
                    pass
            except:
                pass # just exit

        # pass the same arguments to the same script, only prefixing them with dash

        startup_py = popen(python, os_path.join(pmnc_dir, "startup.py"), "-", node, cage,
                           default_log_level or "-", restarting_after_failure and "FAILURE" or "NORMAL")

        # any output from the secondary script is ignored

        stdout_reader = LightThread(target = drain_stream, args = (startup_py.stdout, ))
        stdout_reader.start()

        stderr_reader = LightThread(target = drain_stream, args = (startup_py.stderr, ))
        stderr_reader.start()

        # wait for the secondary script to terminate

        while startup_py.poll() is None:
            try:
                sleep(3.0) # fails with "interrupted system call" at logoff when started as win32 service
            except:
                pass

        stdout_reader.join(3.0) # should have exited with eof
        stderr_reader.join(3.0) # or broken pipe, but who knows

        if startup_py.wait() != 0:
            restarting_after_failure = True # set the flag and restart the secondary script
        else:
            break # successful exit

#######################################

def secondary_startup(node, cage, default_log_level, mode):

    cage_dir = os_path.join(cages_dir, cage)
    logs_dir = os_path.join(cage_dir, "logs")

    log_priorities = { "ERROR": 1, "WARNING": 3, "LOG": 4, "INFO": 5, "DEBUG": 6 }
    log_levels = { 1: "ERR", 2: "MSG", 3: "WRN", 4: "LOG", 5: "INF", 6: "DBG" }
    log_encoding = "windows-1251"
    log_translate = b"         \t                      " + bytes(range(32, 256))
    log_lock = Lock()

    log_yyyymmdd = None
    log_file = None

    # the following function will serve for all cage's logging

    def log(message, *, priority):

        nonlocal log_yyyymmdd, log_file

        line_time = time()
        line_yyyymmdd, line_hhmmss = strftime("%Y%m%d %H:%M:%S", localtime(line_time)).split(" ")
        log_line = "{0:s}.{1:02d} {2:s} [{3:s}] {4:s}".format(line_hhmmss, int(line_time * 100) % 100,
                                                              log_levels.get(priority, "???"),
                                                              current_thread().name, message)
        with log_lock:

            if line_yyyymmdd != log_yyyymmdd: # rotate log file
                try:
                    new_log_file_name = os_path.join(logs_dir, "{0:s}-{1:s}.log".format(cage, line_yyyymmdd))
                    new_log_file = fopen(new_log_file_name, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
                except:
                    pass # if rotation fails, previous log file will still be used
                else:
                    try:
                        close(log_file)
                    except:
                        pass # this also catches the attempt to close None
                    log_file = new_log_file
                    log_yyyymmdd = line_yyyymmdd

            if log_file is not None:
                if message:
                    write(log_file, log_line.encode(log_encoding, "replace").translate(log_translate) + b"\n")
                if priority == 1:
                    fsync(log_file)

    ###################################

    # create loader instance using initial default logging level

    pmnc = ModuleLoader(node, cage, cage_dir, log,
                        log_priorities[default_log_level if default_log_level != "-" else "INFO"])

    # command line argument for logging level takes precedence over
    # the value from config_interfaces, for backwards compatibility

    def update_log_priority():
        log_level = default_log_level if default_log_level != "-" else \
                    pmnc.config_interfaces.get("log_level", default_log_level)
        try:
            log_priority = log_priorities[log_level]
        except KeyError:
            log_priority = log_priorities["INFO"]
        pmnc.set_log_priority(log_priority)

    ###################################

    current_thread().name = "startup"

    if mode == "NORMAL":
        log("the cage is starting up", priority = 2)
    elif mode == "FAILURE":
        log("the cage is restarting after a failure", priority = 2)

    ###################################

    if win32_com:
        _main_thread_id = GetCurrentThreadId()

    def cage_thread_proc():
        try:
            update_log_priority()
            pmnc.startup.start()
            try:
                while not pmnc.startup.wait(3.0):
                    log("", priority = 1) # force flush of a log file
                    update_log_priority()
            except:
                pmnc.log.error(exc_string()) # log and ignore
            finally:
                pmnc.startup.stop()
        finally:
            if win32_com: # release the main thread blocked in PumpMessages
                PostThreadMessage(_main_thread_id, WM_QUIT)

    cage_thread = HeavyThread(target = cage_thread_proc, name = "cage")
    cage_thread.start()

    ###################################

    def termination_watchdog_proc():
        try:
            while stdout.write("\n") > 0:
                stdout.flush()
                sleep(3.0)
        except:
            pass
        finally:
            pmnc.startup.exit()

    termination_watchdog = HeavyThread(target = termination_watchdog_proc, name = "stdout")
    termination_watchdog.start()

    ###################################

    # wait for the cage thread to detect shutdown and terminate

    if win32_com:
        PumpMessages() # in the meanwhile become a message pump

    cage_thread.join()

    ###################################

    log("the cage has been properly shut down", priority = 2)
    log("", priority = 1) # force flush of a log file

#######################################

if argv[1] != "-":
    primary_startup(*argv[1:])
else:
    secondary_startup(*argv[2:])

###############################################################################
# EOF
