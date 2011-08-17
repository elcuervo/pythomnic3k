#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# This module contains implementation of a reverse RPC interface, it constantly
# polls the configured source cages using regular RPC and fetches calls to this
# cage. The fetched requests are executed and the result is submitted back to
# the originating source cage. Note that there could only one such source cage
# with any given name, because otherwise the result may be submitted to a wrong
# instance. Multiple instances of the same reverse RPC source cage need to have
# unique names, perhaps like source_cage_1, source_cage_2 etc.
#
# Reverse RPC is useful in scenarios where a cage residing in DMZ needs
# to call a cage residing in LAN, but the firewall only allows connections
# from LAN to DMZ. Then you configure a reverse RPC interface on the LAN cage,
# and it keeps polling the DMZ cage for calls using regular RPC connections.
# Note that time on both cages should be well synchronized.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Interface" ]

###############################################################################

import threading; from threading import current_thread

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, optional, by_regex, tuple_of
import pmnc.threads; from pmnc.threads import HeavyThread
import pmnc.request; from pmnc.request import fake_request
import pmnc.thread_pool; from pmnc.thread_pool import WorkUnitTimedOut

###############################################################################

valid_cage_name = by_regex("^[A-Za-z0-9_-]{1,32}$")

###############################################################################

class Interface: # reverse RPC interface

    @typecheck
    def __init__(self, name: str, *,
                 source_cages: tuple_of(valid_cage_name),
                 request_timeout: optional(float) = None,
                 **kwargs):

        self._name = name
        self._source_cages = source_cages

        self._request_timeout = request_timeout or \
                                pmnc.config_interfaces.get("request_timeout") # this is now static

        if pmnc.request.self_test != __name__:
            self._poll = lambda cage: pmnc(cage).reverse_call.poll()
            self._post = lambda cage, request_id, response: pmnc(cage).reverse_call.post(request_id, response)
        else: # self-test
            self._process_revrpc_request = kwargs["process_revrpc_request"]
            self._poll = kwargs["poll"]
            self._post = kwargs["post"]

    name = property(lambda self: self._name)

    ###################################

    def start(self): # a separate thread is started to poll each source cage

        self._pollers = []
        for source_cage in self._source_cages:
            poller = HeavyThread(target = self._poller_proc, args = (source_cage, ),
                                 name = "{0:s}:{1:s}".format(self._name, source_cage))
            self._pollers.append(poller)

        for poller in self._pollers:
            poller.start()

    def cease(self):
        for poller in self._pollers:
            poller.stop()

    def stop(self):
        pass

    ###################################
    # a separate thread is executing the following method
    # for each of the configured source cages to poll

    def _poller_proc(self, source_cage):

        while not current_thread().stopped():
            try:

                # attach a new fake request to this thread so that
                # network access in RPC call times out properly

                fake_request(timeout = self._request_timeout, interface = self._name)
                pmnc.request.describe("polling cage {0:s}".format(source_cage))

                # fetch a request from the remote cage
                # and extract the call information from it

                try:
                    revrpc_request = self._poll(source_cage)
                except:
                    pmnc.log.error(exc_string())                    # failure to fetch a request
                    current_thread().stopped(self._request_timeout) # results in idle delay
                    continue

                if revrpc_request is None:
                    continue
                elif current_thread().stopped():
                    break

                assert revrpc_request.pop("target_cage") == __cage__, "expected call to this cage"
                assert revrpc_request.pop("source_cage") == source_cage, "expected call from {0:s}".format(source_cage)

                self._process_request(source_cage, revrpc_request)

            except:
                pmnc.log.error(exc_string())

    ###################################

    def _process_request(self, source_cage, revrpc_request):

        module = revrpc_request["module"]
        method = revrpc_request["method"]

        request_description = "reverse RPC call {0:s}.{1:s} from {2:s}".\
                              format(module, method, source_cage)
        pmnc.request.describe(request_description)

        args = revrpc_request["args"]
        kwargs = revrpc_request["kwargs"]

        # extract the original request, note that its remaining time
        # is decreased if the current RPC request has less time left

        request_dict = revrpc_request["request"]
        request_id = request_dict["unique_id"]

        request_dict.setdefault("interface", "revrpc") # backwards compatibility section
        request_dict.setdefault("protocol", "revrpc")
        request_dict["parameters"].setdefault("auth_tokens", {}).\
                                   setdefault("source_cage", source_cage)

        # create a new request for processing the message and enqueue it,
        # note that the interface doesn't wait for the requests to complete

        request = pmnc.interfaces.begin_request(
                    timeout = self._request_timeout,
                    interface = self._name, protocol = "revrpc",
                    parameters = dict(auth_tokens = dict()),
                    description = request_description)

        pmnc.interfaces.enqueue(request, self.wu_process_request,
                                (source_cage, module, method, args, kwargs, request_dict))

    ###################################
    # this method is a work unit executed by one of the interface pool threads,
    # it does the processing and submits the result back to the originating cage

    @typecheck
    def wu_process_request(self, source_cage: valid_cage_name, module: str, method: str,
                           args: tuple, kwargs: dict, request_dict: dict):

        try:

            # see for how long the request was on the execution queue up to this moment
            # and whether it has expired in the meantime, if it did there is no reason
            # to proceed and we simply bail out

            if pmnc.request.expired:
                pmnc.log.error("request has expired and will not be processed")
                success = False
                return # goes through finally section below

            # create a copy of and impersonate the original request, note that
            # the timeout is adjusted if current request has less time left

            request = pmnc.request.from_dict(request_dict, timeout = pmnc.request.remain)

            try:
                pmnc.log.debug("impersonating request {0:s}".format(request.description))
                original_request = current_thread()._request
                current_thread()._request = request
                try:
                    with pmnc.performance.request_processing():
                        result = self._process_revrpc_request(module, method, args, kwargs)
                finally:
                    current_thread()._request = original_request
            except:
                error = exc_string()
                pmnc.log.error("incoming reverse RPC call failed: {0:s}".format(error))
                revrpc_response = dict(exception = error)
                deliver_message = "reverse RPC error"
            else:
                revrpc_response = dict(result = result)
                deliver_message = "reverse RPC response"

            # submit the response back to the source cage using the request's unique id,
            # note that the delivery is done on behalf and within the local request

            pmnc.log.debug("delivering {0:s}".format(deliver_message))
            self._post(source_cage, request_dict["unique_id"], revrpc_response)

        except:
            pmnc.log.error(exc_string()) # log and ignore
            success = False
        else:
            pmnc.log.debug("{0:s} has been delivered".format(deliver_message))
            success = True
        finally:                                 # the request ends itself
            pmnc.interfaces.end_request(success) # possibly way after deadline

    ###################################

    def _process_revrpc_request(self, module, method, args, kwargs):
        return pmnc.__getattr__(module).__getattr__(method)(*args, **kwargs)

###############################################################################

def self_test():

    from interlocked_queue import InterlockedQueue
    from time import time, sleep
    from pmnc.self_test import active_interface

    ###################################

    poll_queue = InterlockedQueue()
    post_queue = InterlockedQueue()

    cage_queues = dict \
    (
        source_cage = (poll_queue, post_queue),
    )

    test_interface_config = dict \
    (
    protocol = "revrpc",
    source_cages = ("source_cage", ),
    poll = lambda cage: pmnc.request.pop(cage_queues[cage][0]),
    post = lambda cage, request_id, response: cage_queues[cage][1].push((request_id, response)),
    )

    def interface_config(**kwargs):
        result = test_interface_config.copy()
        result.update(kwargs)
        return result

    ###################################

    def test_success():

        def process_revrpc_request(module, method, args, kwargs):
            return module, method, args, kwargs, pmnc.request.to_dict()

        with active_interface("revrpc", **interface_config(process_revrpc_request = process_revrpc_request)):

            fake_request(3.0)
            pmnc.request.describe("my request")

            pmnc.request.parameters["foo"] = "bar"
            request_dict = pmnc.request.to_dict()

            request = dict(source_cage = "source_cage",
                           target_cage = __cage__,
                           module = "module",
                           method = "method",
                           args = (1, "2"),
                           kwargs = { "biz": "baz" },
                           request = request_dict)

            poll_queue.push(request)
            request_id, response = post_queue.pop(3.0)
            assert request_id == pmnc.request.unique_id

            module, method, args, kwargs, request = response.pop("result")
            assert not response
            assert module == "module" and method == "method" and args == (1, "2") and kwargs == { "biz": "baz" }

            deadline = request.pop("deadline")
            assert abs(deadline - (time() + pmnc.request.remain)) < 0.01

            assert request == dict \
                              (
                                  protocol = pmnc.request.protocol,
                                  description = "my request",
                                  parameters = dict(auth_tokens = { "source_cage": "source_cage" }, foo = "bar"),
                                  interface = pmnc.request.interface,
                                  unique_id = pmnc.request.unique_id,
                              )

    test_success()

    ###################################

    def test_failure():

        def process_revrpc_request(module, method, args, kwargs):
            1 / 0

        with active_interface("revrpc", **interface_config(process_revrpc_request = process_revrpc_request)):

            fake_request(3.0)

            request_dict = pmnc.request.to_dict()

            request = dict(source_cage = "source_cage",
                           target_cage = __cage__,
                           module = "module",
                           method = "method",
                           args = (),
                           kwargs = {},
                           request = request_dict)

            poll_queue.push(request)
            request_id, response = post_queue.pop(3.0)
            assert request_id == pmnc.request.unique_id

            error = response.pop("exception")
            assert not response
            assert error.startswith("ZeroDivisionError(")

    test_failure()

    ###################################

    def test_timeout():

        def process_revrpc_request(module, method, args, kwargs):
            sleep(4.0)
            return "ok"

        with active_interface("revrpc", **interface_config(process_revrpc_request = process_revrpc_request)):

            fake_request(3.0)

            request_dict = pmnc.request.to_dict()

            request = dict(source_cage = "source_cage",
                           target_cage = __cage__,
                           module = "module",
                           method = "method",
                           args = (),
                           kwargs = {},
                           request = request_dict)

            poll_queue.push(request)
            assert pmnc.request.pop(post_queue) is None
            request_id, response = post_queue.pop(2.0)
            assert response == { "result": "ok" }

    test_timeout()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF