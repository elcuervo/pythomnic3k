#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module periodically sends probes to all cages known to this cage's
# RPC interface. Supposedly there is just one health monitor on the network,
# although nothing prevents two or more, if so the regular cages will simply
# receive more probes.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "HealthMonitor" ]

################################################################################

import os; from os import urandom
import time; from time import strftime
import binascii; from binascii import b2a_hex
import threading; from threading import current_thread

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, optional, one_of
import interlocked_queue; from interlocked_queue import InterlockedQueue
import pmnc.threads; from pmnc.threads import HeavyThread
import pmnc.request; from pmnc.request import Request
import pmnc.timeout; from pmnc.timeout import Timeout

################################################################################

class HealthMonitor:

    def __init__(self, **kwargs):

        self._rpc_interface = pmnc.interfaces.get_interface("rpc")
        if self._rpc_interface is None:
            raise Exception("health monitor requires enabled rpc interface")

        self._probe_thread_pool = pmnc.shared_pools.get_private_thread_pool()
        self._up_cages = {} # { cage: { node: { location: ..., probe_result: ... } } }
        self._up_down_queue = InterlockedQueue()

        self._request_timeout = pmnc.config_interfaces.get("request_timeout") # this is now static

        if pmnc.request.self_test == __name__: # self-test
            self._process_event = kwargs["process_event"]
            self._probe_cage = kwargs["probe_cage"]

    ###################################

    def start(self):
        self._probe_thread = HeavyThread(target = self._probe_thread_proc,
                                         name = "health_monitor:probe") # always called "health_monitor"
        self._probe_thread.start()

    def stop(self):
        self._probe_thread.stop()

    ###################################

    # this method is executed in a private thread and is scheduling probe calls
    # to cages known to the RPC interface or previously probed and found to be up

    def _probe_thread_proc(self):

        per_cage_interval = 0.0

        # calls to _poll_up_down_queue are interleaved and allow this thread
        # to maintain structures such as _up_cages in response to events
        # posted by the probe threads to the _up_down_queue

        while self._poll_up_down_queue(per_cage_interval):
            try:

                # extract all cages currently known to the rpc interface and
                # merge them with cages previously probed and found to be up,
                # except for the health_monitor cage itself should be skipped

                probe_cages = \
                    { known_cage: { known_node: dict(location = known_location, probe_result = None)
                                    for known_node, known_location in self._rpc_interface.get_nodes(known_cage).items() }
                      for known_cage in self._rpc_interface.get_cages() if known_cage != "health_monitor" }

                self._merge_cages(probe_cages, self._up_cages)

                probe_period = pmnc.config.get("probe_period")
                per_cage_interval = probe_period / (len(probe_cages) + 1)

                # walk through all cages to be probed and schedule calls to probe
                # to a private thread pool using fake unregistered requests

                for cage, nodes in probe_cages.items():

                    for node, cage_info in nodes.items():

                        cage_location = cage_info["location"]

                        # note that the requests created here are not registered with
                        # interfaces and enqueued to a different pool too, they are
                        # therefore entitled to termination without warning at shutdown,
                        # this is ok, because they do no useful work for the clients

                        request = Request(timeout = self._request_timeout,
                                          interface = "__health_monitor__", protocol = "n/a",
                                          parameters = dict(auth_tokens = dict()),
                                          description = "probing cage {0:s} at {1:s}".format(cage, cage_location))

                        self._probe_thread_pool.enqueue(request, self.wu_probe_cage,
                                (node, cage, cage_location, cage_info["probe_result"]), {})

                    # then again yield to polling the queue for a while

                    if not self._poll_up_down_queue(per_cage_interval):
                        break

            except:
                pmnc.log.error(exc_string()) # log and ignore

    ###################################

    # this method merges cages known to the RPC interface with cages
    # previously probed and known to be up, such merging is necessary
    # because if a cage dies just before its next advertisement broadcast,
    # it would disappear from known, but will not be probed again and
    # hence thought to be up forever

    @staticmethod
    def _merge_cages(known_cages: dict, up_cages: dict):

        probe_cages = known_cages # merging in place

        for up_cage, up_nodes in up_cages.items():
            for up_node, up_cage_info in up_nodes.items():
                probe_nodes = probe_cages.setdefault(up_cage, {})
                if up_node in probe_nodes:
                    cage_info = probe_nodes[up_node]
                    if cage_info["location"] == up_cage_info["location"]:
                        cage_info.update(probe_result = up_cage_info["probe_result"])
                    else:
                        cage_info.update(probe_result = "restarted") # note this case
                else:
                    probe_nodes[up_node] = up_cage_info

    ###################################

    # a call to this method is enqueued to a private thread pool
    # for each cage to probe on every pass of _probe_thread

    def wu_probe_cage(self, node, cage, location, prev_probe_result):

        if pmnc.request.expired: # no need to report anything for a probing request
            return

        pmnc.log.debug("sending probe")
        try:
            probe_result = self._probe_cage(node, cage, location)
        except:
            pmnc.log.warning("probe failed: {0:s}".format(exc_string()))
            self._up_down_queue.push((node, cage, "down"))
        else:
            pmnc.log.debug("probe returned successfully")
            if prev_probe_result == "restarted":               # if the cage has restarted
                self._up_down_queue.push((node, cage, "down")) # we push "down" event first
            self._up_down_queue.push((node, cage, "up", location, probe_result))

    ###################################

    # this method is invoked by one of the private pool threads
    # to send the actual probe call to the cage being probed

    @typecheck
    def _probe_cage(self, node, cage, location) -> dict:

        # health monitor has to create rpc resources manually, not using
        # pmnc(cage) syntax, because we need to access exact cage at exact
        # node and location (i.e. host and port) and to avoid discovery

        connect_timeout = pmnc.config_resource_rpc.get("discovery_timeout")

        rpc = pmnc.protocol_rpc.Resource("{0:s}.{1:s}".format(node, cage),
                                         broadcast_address = ("n/a", 0),
                                         discovery_timeout = connect_timeout,
                                         multiple_timeout_allowance = 0.0,
                                         flock_id = "unused",
                                         exact_locations = { cage: location }, # this prevents discovery
                                         pool__resource_name = cage)

        rpc.connect()
        try:
            rpc.begin_transaction("", source_module_name = __name__, transaction_options = {},
                                  resource_args = (), resource_kwargs = {})
            try:
                probe_result = rpc.health_monitor_event.probe() # there, an RPC call
            except:
                rpc.rollback()
                raise
            else:
                rpc.commit()
        finally:
            rpc.disconnect()

        return probe_result # if the cage returns anything but a dict, it is considered a failure

    ###################################

    # this method is called by the _probe_thread during its idle times
    # to fetch up/down events posted to the _up_down_queue by the probe
    # threads and in response to maintain structures such as _up_cages

    def _poll_up_down_queue(self, timeout: float) -> bool: # returns "should keep running"

        poll_timeout = Timeout(timeout)
        while not poll_timeout.expired:

            pop_timeout = Timeout(min(poll_timeout.remain, 1.0))
            while not pop_timeout.expired:

                event = pop_timeout.pop(self._up_down_queue)
                if event is not None:
                    try:

                        node, cage, up_down, *args = event
                        if up_down == "up":

                            location, probe_result = args

                            # add the cage to cages known to be up and schedule
                            # application notification call if it was down or
                            # returned a different probe result

                            cage_info = self._up_cages.setdefault(cage, {}).setdefault(node, {})
                            if not cage_info or cage_info["probe_result"] != probe_result:
                                self._schedule_up_down_event(node, cage, "up", probe_result)
                            cage_info.update(location = location, probe_result = probe_result)

                        elif up_down == "down":

                            # remove the cage from cages known to be up and schedule
                            # application notification call it was up

                            if self._up_cages.setdefault(cage, {}).pop(node, None):
                                self._schedule_up_down_event(node, cage, "down")

                    except:
                        pmnc.log.error(exc_string()) # log and ignore

            if current_thread().stopped():
                return False

        return True

    ###################################

    # this method is called by the _probe_thread in response to change
    # of some cage's state detected in _poll_up_down_queue

    def _schedule_up_down_event(self, node, cage, up_down, probe_result = None):

        # application notification invokes methods from health_monitor_event module
        # and must be executed just like a regular request from some interface

        request = pmnc.interfaces.begin_request(
                    timeout = self._request_timeout,
                    interface = "__health_monitor__", protocol = "n/a",
                    parameters = dict(auth_tokens = dict()),
                    description = "cage {0:s}.{1:s} is {2:s}".format(node, cage, up_down))

        # note that this request is not waited upon

        pmnc.interfaces.enqueue(request, self.wu_process_event, (node, cage, up_down, probe_result))

    ###################################

    # this method is invoked by one of the interfaces pool threads to register
    # the event of some cage going up or down by calling an appropriate method
    # from the health_monitor_event module

    @typecheck
    def wu_process_event(self, node: str, cage: str, up_down: one_of("up", "down"),
                         probe_result: optional(dict)):
        try:

            # see for how long the request was on the execution queue up to this moment
            # and whether it has expired in the meantime, if it did there is no reason
            # to proceed and we simply bail out

            if pmnc.request.expired:
                pmnc.log.error("request has expired and will not be processed")
                success = False
                return # goes through finally section below

            with pmnc.performance.request_processing():
                self._process_event(node, cage, up_down, probe_result)

        except:
            pmnc.log.error(exc_string()) # log and ignore
            success = False
        else:
            success = True
        finally:                                 # the request ends itself
            pmnc.interfaces.end_request(success) # possibly way after deadline

    ###################################

    def _process_event(self, node, cage, up_down, probe_result):

        if up_down == "up":
            pmnc.health_monitor_event.cage_up(node, cage, probe_result)
        elif up_down == "down":
            pmnc.health_monitor_event.cage_down(node, cage)

###############################################################################

def self_test():

    from threading import Event

    ###################################

    def test_merge_cages():

        def known(*args):
            result = {}
            for c, n, l in args:
                result.setdefault(c, {})[n] = dict(location = l, probe_result = None)
            return result

        def up(*args):
            result = {}
            for c, n, l, r in args:
                result.setdefault(c, {})[n] = dict(location = l, probe_result = r)
            return result

        probe = up

        def merge(d1, d2):
            HealthMonitor._merge_cages(d1, d2)
            return d1

        assert merge(known(), up()) == {}

        assert merge(known(("cage1", "nodeA", "ssl://1:A")), up()) == probe(("cage1", "nodeA", "ssl://1:A", None))
        assert merge(known(), up(("cage1", "nodeA", "ssl://1:A", {}))) == probe(("cage1", "nodeA", "ssl://1:A", {}))
        assert merge(known(("cage1", "nodeA", "ssl://1:A")), up(("cage1", "nodeA", "ssl://1:A", {}))) == probe(("cage1", "nodeA", "ssl://1:A", {}))
        assert merge(known(("cage1", "nodeA", "ssl://1:AX")), up(("cage1", "nodeA", "ssl://1:A", {}))) == probe(("cage1", "nodeA", "ssl://1:AX", "restarted"))

        assert merge(known(("cage1", "nodeA", "ssl://1:A"), ("cage1", "nodeB", "ssl://1:B")), up()) == \
               probe(("cage1", "nodeA", "ssl://1:A", None), ("cage1", "nodeB", "ssl://1:B", None))

        assert merge(known(("cage1", "nodeA", "ssl://1:A"), ("cage1", "nodeB", "ssl://1:B")), up(("cage1", "nodeB", "ssl://1:B", {}))) == \
               probe(("cage1", "nodeA", "ssl://1:A", None), ("cage1", "nodeB", "ssl://1:B", {}))

        assert merge(known(("cage1", "nodeA", "ssl://1:A"), ("cage1", "nodeB", "ssl://1:B")), up(("cage1", "nodeA", "ssl://1:A", {}))) == \
               probe(("cage1", "nodeA", "ssl://1:A", {}), ("cage1", "nodeB", "ssl://1:B", None))

        assert merge(known(("cage1", "nodeA", "ssl://1:A")), up(("cage1", "nodeA", "ssl://1:A", {}), ("cage1", "nodeB", "ssl://1:B", {}))) == \
               probe(("cage1", "nodeA", "ssl://1:A", {}), ("cage1", "nodeB", "ssl://1:B", {}))

        assert merge(known(("cage1", "nodeA", "ssl://1:A"), ("cage2", "nodeA", "ssl://2:A")), up()) == \
               probe(("cage1", "nodeA", "ssl://1:A", None), ("cage2", "nodeA", "ssl://2:A", None))

        assert merge(known(("cage1", "nodeA", "ssl://1:A"), ("cage2", "nodeA", "ssl://2:A")), up(("cage1", "nodeA", "ssl://1:A", {}))) == \
               probe(("cage1", "nodeA", "ssl://1:A", {}), ("cage2", "nodeA", "ssl://2:A", None))

        assert merge(known(("cage1", "nodeA", "ssl://1:A"), ("cage2", "nodeA", "ssl://2:A")), up(("cage2", "nodeA", "ssl://2:AX", {}))) == \
               probe(("cage1", "nodeA", "ssl://1:A", None), ("cage2", "nodeA", "ssl://2:A", "restarted"))

        assert merge(known(("cage1", "nodeA", "ssl://1:A"), ("cage1", "nodeB", "ssl://1:B"),
                           ("cage2", "nodeA", "ssl://2:A"), ("cage2", "nodeB", "ssl://2:B")),
                     up(("cage1", "nodeA", "ssl://1:A", {}), ("cage2", "nodeB", "ssl://2:B", {}))) == \
               probe(("cage1", "nodeA", "ssl://1:A", {}), ("cage1", "nodeB", "ssl://1:B", None),
                     ("cage2", "nodeA", "ssl://2:A", None), ("cage2", "nodeB", "ssl://2:B", {}))

    test_merge_cages()

    ###################################

    class FakeRpcInterface:

        @typecheck
        def __init__(self, cages_list):
            self._cages_list = cages_list
            self._queue = InterlockedQueue()
            self._pass = 0
            self._stopped = Event()

        def get_cages(self):
            try:
                self._cages = self._cages_list[self._pass]
            except IndexError:
                self._cages = {}
                self._stopped.set()
            self._pass += 1
            return list(self._cages.keys())

        def get_nodes(self, cage):
            return self._cages[cage]

        def process_event(self, node, cage, up_down, probe_result):
            self._queue.push((node, cage, up_down, probe_result))

        def extract_events(self):
            result = {}
            event = self._queue.pop(1.0)
            while event is not None:
                node, cage, up_down, probe_result = event
                events = result.setdefault("{0:s}.{1:s}".format(node, cage), [])
                if up_down == "up":
                    events.append(probe_result)
                else:
                    events.append(None)
                event = self._queue.pop(1.0)
            return result

    ###################################

    def start_health_monitor(cages_list, responses_list):

        rpc_interface = FakeRpcInterface(cages_list)
        pmnc.interfaces.set_fake_interface("rpc", rpc_interface)

        def _probe_cage(node, cage, location):
            result = responses_list[rpc_interface._pass-1].get(location)
            if result is not None:
                return result
            else:
                raise Exception("down")

        hm = pmnc.health_monitor.HealthMonitor(process_event = rpc_interface.process_event,
                                               probe_cage = _probe_cage)
        hm.wait = rpc_interface._stopped.wait
        hm.start()

        return hm

    ###################################

    def stop_health_monitor(hm):

        hm.stop()
        pmnc.interfaces.delete_fake_interface("rpc")

    ###################################

    def simulate_scenario(cages_list, responses_list):

        hm = start_health_monitor(cages_list, responses_list)
        try:
            hm.wait()
        finally:
            stop_health_monitor(hm)

        return hm._rpc_interface.extract_events()

    ###################################

    def test_one_up():

        cages_1 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_list = [cages_1]

        responses_1 = dict(ssl_1A = {})
        responses_2 = dict(ssl_1A = {})
        responses_list = [responses_1, responses_2]

        assert simulate_scenario(cages_list, responses_list) == \
               { "nodeA.cage1": [{}] }

    test_one_up()

    ###################################

    def test_one_up_long():

        cages_1 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_2 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_3 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_list = [cages_1, cages_2, cages_3]

        responses_1 = dict(ssl_1A = {})
        responses_2 = dict(ssl_1A = {})
        responses_3 = dict(ssl_1A = {})
        responses_4 = dict(ssl_1A = {})
        responses_list = [responses_1, responses_2, responses_3, responses_4]

        assert simulate_scenario(cages_list, responses_list) == \
               { "nodeA.cage1": [{}] }

    test_one_up_long()

    ###################################

    def test_one_up_down():

        cages_1 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_2 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_3 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_4 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_5 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_list = [cages_1, cages_2, cages_3, cages_4, cages_5]

        responses_1 = dict(ssl_1A = {})
        responses_2 = dict(ssl_1A = {})
        responses_3 = dict()
        responses_4 = dict()
        responses_5 = dict(ssl_1A = {})
        responses_6 = dict(ssl_1A = {})
        responses_list = [responses_1, responses_2, responses_3, responses_4, responses_5, responses_6]

        assert simulate_scenario(cages_list, responses_list) == \
               { "nodeA.cage1": [{}, None, {}] }

    test_one_up_down()

    ###################################

    def test_one_changes():

        cages_1 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_2 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_3 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_list = [cages_1, cages_2, cages_3]

        responses_1 = dict(ssl_1A = {"key": 1})
        responses_2 = dict(ssl_1A = {"key": 1})
        responses_3 = dict(ssl_1A = {"key": 2})
        responses_4 = dict(ssl_1A = {"key": 2})
        responses_list = [responses_1, responses_2, responses_3, responses_4]

        assert simulate_scenario(cages_list, responses_list) == \
               { "nodeA.cage1": [{"key": 1}, {"key": 2}] }

    test_one_changes()

    ###################################

    def test_one_restarts():

        cages_1 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_2 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_3 = dict(cage1 = dict(nodeA = "ssl_1AX"))
        cages_4 = dict(cage1 = dict(nodeA = "ssl_1AX"))
        cages_list = [cages_1, cages_2, cages_3, cages_4]

        responses_1 = dict(ssl_1A = {})
        responses_2 = dict(ssl_1A = {})
        responses_3 = dict(ssl_1AX = {})
        responses_4 = dict(ssl_1AX = {})
        responses_5 = dict(ssl_1AX = {})
        responses_list = [responses_1, responses_2, responses_3, responses_4, responses_5]

        assert simulate_scenario(cages_list, responses_list) == \
               { "nodeA.cage1": [{}, None, {}] }

    test_one_restarts()

    ###################################

    def test_one_migrates():

        cages_1 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_2 = dict(cage1 = dict(nodeA = "ssl_1A", nodeB = "ssl_1B"))
        cages_3 = dict(cage1 = dict(nodeA = "ssl_1A", nodeB = "ssl_1B"))
        cages_4 = dict(cage1 = dict(nodeB = "ssl_1B"))
        cages_list = [cages_1, cages_2, cages_3, cages_4]

        responses_1 = dict(ssl_1A = {})
        responses_2 = dict(ssl_1A = {}, ssl_1B = {})
        responses_3 = dict(ssl_1A = {}, ssl_1B = {})
        responses_4 = dict(ssl_1B = {})
        responses_5 = dict(ssl_1B = {})
        responses_list = [responses_1, responses_2, responses_3, responses_4, responses_5]

        assert simulate_scenario(cages_list, responses_list) == \
               { "nodeA.cage1": [{}, None], "nodeB.cage1": [{}] }

    test_one_migrates()

    ###################################

    def test_one_stops():

        cages_1 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_2 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_3 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_4 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_list = [cages_1, cages_2, cages_3, cages_4]

        responses_1 = dict(ssl_1A = {})
        responses_2 = dict(ssl_1A = {})
        responses_3 = dict()
        responses_4 = dict()
        responses_5 = dict()
        responses_list = [responses_1, responses_2, responses_3, responses_4, responses_5]

        assert simulate_scenario(cages_list, responses_list) == \
               { "nodeA.cage1": [{}, None] }

    test_one_stops()

    ###################################

    def test_one_skips_broadcasting():

        cages_1 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_2 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_3 = dict()
        cages_4 = dict()
        cages_5 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_6 = dict(cage1 = dict(nodeA = "ssl_1A"))
        cages_list = [cages_1, cages_2, cages_3, cages_4, cages_5, cages_6]

        responses_1 = dict(ssl_1A = {})
        responses_2 = dict(ssl_1A = {})
        responses_3 = dict(ssl_1A = {})
        responses_4 = dict(ssl_1A = {})
        responses_5 = dict(ssl_1A = {})
        responses_6 = dict(ssl_1A = {})
        responses_7 = dict(ssl_1A = {})
        responses_list = [responses_1, responses_2, responses_3, responses_4, responses_5, responses_6, responses_7]

        assert simulate_scenario(cages_list, responses_list) == \
               { "nodeA.cage1": [{}] }

    test_one_skips_broadcasting()

    ###################################

    def test_two_swap():

        cages_1 = dict(cage1 = dict(nodeA = "ssl_1A"), cage2 = dict(nodeB = "ssl_2B"))
        cages_2 = dict(cage1 = dict(nodeA = "ssl_1A"), cage2 = dict(nodeB = "ssl_2B"))
        cages_3 = dict(cage1 = dict(nodeA = "ssl_1A"), cage2 = dict(nodeB = "ssl_2B"))
        cages_4 = dict(cage1 = dict(nodeA = "ssl_1A"), cage2 = dict(nodeB = "ssl_2B"))
        cages_5 = dict(cage1 = dict(nodeB = "ssl_1B"), cage2 = dict(nodeA = "ssl_2A"))
        cages_6 = dict(cage1 = dict(nodeB = "ssl_1B"), cage2 = dict(nodeA = "ssl_2A"))
        cages_list = [cages_1, cages_2, cages_3, cages_4, cages_5, cages_6]

        responses_1 = dict(ssl_1A = {}, ssl_2B = {})
        responses_2 = dict(ssl_1A = {}, ssl_2B = {})
        responses_3 = dict()
        responses_4 = dict(ssl_1B = {}, ssl_2A = {})
        responses_5 = dict(ssl_1B = {}, ssl_2A = {})
        responses_6 = dict(ssl_1B = {}, ssl_2A = {})
        responses_7 = dict(ssl_1B = {}, ssl_2A = {})
        responses_list = [responses_1, responses_2, responses_3, responses_4, responses_5, responses_6, responses_7]

        assert simulate_scenario(cages_list, responses_list) == \
               { "nodeA.cage1": [{}, None], "nodeA.cage2": [{}],
                 "nodeB.cage2": [{}, None], "nodeB.cage1": [{}] }

    test_two_swap()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF