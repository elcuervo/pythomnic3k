#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module contains a set of application-level hooks for handling events
# associated with health monitor.
#
# Pythomnic3k project
# (c) 2005-2009, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "probe", "notify", "cage_up", "cage_down" ]

################################################################################

import time; from time import time, localtime, strftime

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import typecheck; from typecheck import by_regex, one_of

###############################################################################

valid_node_name = by_regex("^[A-Za-z0-9_-]{1,32}$")
valid_cage_name = by_regex("^[A-Za-z0-9_-]{1,32}$")

################################################################################

# this method is invoked on any cage through RPC call from health_monitor cage,
# it returns a dict with unspecified (performance-related ?) parameters

def probe() -> dict:

    return {}

################################################################################

# this method is invoked on health_monitor cage through retried RPC call
# from some other cage

def notify(timestamp: int, node: valid_node_name, cage: valid_cage_name,
           level: one_of("INFO", "WARNING", "ERROR", "ALERT"), message: str):

    assert __cage__ == "health_monitor"

    current_timestamp = int(time())
    message_ts = localtime(timestamp)
    current_ts = localtime(current_timestamp)

    if abs(timestamp - current_timestamp) <= 5:
        time_mark = ""
    elif message_ts.tm_yday == current_ts.tm_yday:
        time_mark = strftime(" (%H:%M:%S)", message_ts)
    else:
        time_mark = strftime(" (%Y-%m-%d %H:%M:%S)", message_ts)

    pmnc.log.message("[{0:s}] {1:s}.{2:s}{3:s}: {4:s}".\
                     format(level, node, cage, time_mark, message))

###############################################################################

# this method is invoked on health_monitor cage whenever health_monitor
# module detects some cage state changes to up or its probe result changes

def cage_up(node: str, cage: str, probe_result: dict):

    assert __cage__ == "health_monitor"

    pmnc.log.message("{0:s}.{1:s} is up".format(node, cage))

###############################################################################

# this method is invoked on health_monitor cage whenever health_monitor
# module detects some cage state changes to down

def cage_down(node: str, cage: str):

    assert __cage__ == "health_monitor"

    pmnc.log.message("{0:s}.{1:s} is down".format(node, cage))

###############################################################################

def self_test():

    ###################################

    def test_probe():

        assert pmnc.health_monitor_event.probe() == {}

    test_probe()

    ###################################

    global __cage__
    __cage__ = "health_monitor"

    ###################################

    def test_notify():

        pmnc.health_monitor_event.notify(int(time()), "source_node", "source_cage", "INFO", "info")
        pmnc.health_monitor_event.notify(int(time() - 6.0), "source_node", "source_cage", "WARNING", "warning")
        pmnc.health_monitor_event.notify(int(time() - 90000.0), "source_node", "source_cage", "ERROR", "error")
        pmnc.health_monitor_event.notify(int(time() + 3.0), "source_node", "source_cage", "ALERT", "alert")

    test_notify()

    ###################################

    def test_cage_up():

        pmnc.health_monitor_event.cage_up("node", "cage", {})

    test_cage_up()

    ###################################

    def test_cage_down():

        pmnc.health_monitor_event.cage_down("node", "cage")

    test_cage_down()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF