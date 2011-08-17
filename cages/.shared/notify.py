#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module transmits notification messages to the health_monitor cage.
# Last few are kept locally and are visible on interface_performance's web page.
#
# Sample:
# pmnc.notify.info("this will appear in health_monitor's log")
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "info", "warning", "error", "alert", "extract" ]

################################################################################

import time; from time import time
import threading; from threading import Lock

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

################################################################################

def info(message: str):
    _notify("INFO", message)

def warning(message: str):
    _notify("WARNING", message)

def error(message: str):
    _notify("ERROR", message)

def alert(message: str):
    _notify("ALERT", message)

################################################################################
# technically, this is a module-level state, but it's expendable, therefore
# the module is still reloadable

required_interfaces_available = None

notifications = []
notifications_lock = Lock()

################################################################################

def _notify(level, message):

    pmnc.log.message("[{0:s}] {1:s}".format(level, message))

    timestamp = int(time())

    # save the notification in an in-memory circular buffer

    messages_to_keep = pmnc.config.get("messages_to_keep")

    with notifications_lock:
        notifications.append(dict(timestamp = timestamp, level = level, message = message))
        while len(notifications) > messages_to_keep:
            notifications.pop(0)

    if __cage__ != "health_monitor": # this cage is not itself a health monitor

        # see if the interfaces required to send message to a health monitor have been started

        retry_queue = pmnc.config.get("retry_queue")

        global required_interfaces_available

        if required_interfaces_available is None:
            required_interfaces_available = pmnc.interfaces.get_interface("rpc") is not None and \
                                            pmnc.interfaces.get_interface(retry_queue) is not None

        if required_interfaces_available:
            pmnc("health_monitor", queue = retry_queue).\
                health_monitor_event.notify(timestamp, __node__, __cage__, level, message)

###############################################################################

def extract():
    with notifications_lock:
        return notifications[:]

###############################################################################

def self_test():

    from pmnc.request import fake_request

    ###################################

    def test_notify():

        fake_request(10.0)

        pmnc.notify.info("info")
        pmnc.notify.warning("warning")
        pmnc.notify.error("error")
        pmnc.notify.alert("alert")

        assert len(notifications) == 4
        extracted_notifications = pmnc.notify.extract()
        assert extracted_notifications is not notifications
        assert [ (type(d["timestamp"]), d["level"], d["message"]) for d in extracted_notifications ] == \
               [ (int, "INFO", "info"), (int, "WARNING", "warning"), (int, "ERROR", "error"), (int, "ALERT", "alert") ]

        pmnc.notify.alert("again")

        assert len(notifications) == 4
        extracted_notifications = pmnc.notify.extract()
        assert [ (type(d["timestamp"]), d["level"], d["message"]) for d in extracted_notifications ] == \
               [ (int, "WARNING", "warning"), (int, "ERROR", "error"), (int, "ALERT", "alert"), (int, "ALERT", "again") ]

    test_notify()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF