#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module handles reverse RPC calls, in which target cage actively polls
# for incoming calls, rather that passively waiting for them.
#
# On source cage, a reverse RPC call is simply this:
# result = pmnc("target_cage:reverse").module.method(*args, **kwargs)
#
# On target cage, an instance of an "revrpc" interface must be created and
# configured to poll source cage specifically.
#
# The reason for such calls to exist is DMZ scenario where source cage may
# be unable to establish connection to the target cage, but only the other
# way around.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "execute_reverse", "poll", "post" ]
__reloadable__ = False

################################################################################

import threading; from threading import Lock

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import by_regex
import interlocked_queue; from interlocked_queue import InterlockedQueue
import pmnc.resource_pool; from pmnc.resource_pool import ResourceError, RPCError

###############################################################################

valid_cage_name = by_regex("^[A-Za-z0-9_-]{1,32}$")
valid_module_name = by_regex("^[A-Za-z0-9_]{1,64}$")
valid_method_name = by_regex("^[A-Za-z0-9_]{1,64}$")

###############################################################################

# module-level state => not reloadable

_rq_queues = {} # one queue per target cage, permanent
_rq_queues_lock = Lock()

_rs_queues = {} # one queue per active request, transient
_rs_queues_lock = Lock()

###############################################################################

def _get_rq_queue(cage):

    with _rq_queues_lock:
        if cage not in _rq_queues:
            rq_queue = InterlockedQueue()
            _rq_queues[cage] = rq_queue
        else:
            rq_queue = _rq_queues[cage]

    return rq_queue

###############################################################################
# this method is called from module_loader.py to handle reverse RPC call
# pmnc("target_cage:reverse").module.method(*args, **kwargs)

def execute_reverse(target_cage: valid_cage_name, module: valid_module_name,
                    method: valid_method_name, args: tuple, kwargs: dict):

    # wrap up an RPC call identical to how it's done in protocol_rpc.py

    request_dict = pmnc.request.to_dict()

    # remove request parameters that must not cross the RPC border

    request_dict["parameters"].pop("retry", None)

    # wrap all the call parameters in a plain dict

    request = dict(source_cage = __cage__,
                   target_cage = target_cage,
                   module = module, method = method,
                   args = args, kwargs = kwargs,
                   request = request_dict)

    request_description = "reverse RPC request {0:s}.{1:s} to {2:s}". \
                          format(module, method, target_cage)

    # create a one-time response queue just for this request

    rs_queue = InterlockedQueue()
    request_id = pmnc.request.unique_id

    with _rs_queues_lock:
        _rs_queues[request_id] = rs_queue # register the call as being active
    try:

        pmnc.log.info("sending {0:s}".format(request_description))
        try:

            # enqueue the call and wait for response

            rq_queue = _get_rq_queue(target_cage)
            rq_queue.push((request_id, request))

            response = pmnc.request.pop(rs_queue)
            if response is None:
                raise Exception("request deadline waiting for response")

            try:
                result = response["result"]
            except KeyError:
                raise RPCError(description = response["exception"], terminal = False)

        except RPCError as e:
            pmnc.log.warning("{0:s} returned error: {1:s}".\
                             format(request_description, e.description))
            raise
        except:
            pmnc.log.warning("{0:s} failed: {1:s}".\
                             format(request_description, exc_string()))
            ResourceError.rethrow(recoverable = False)
        else:
            pmnc.log.info("reverse RPC request returned successfully")
            return result

    finally:
        with _rs_queues_lock:
            del _rs_queues[request_id] # unregister the call

###############################################################################
# other cages call this method to fetch calls enqueued to them in call()

_poll_response_threshold = 3.0 # this accounts for time differences and for the
                               # response delivery time as well, so beware
def poll():

    poll_timeout = pmnc.request.remain - _poll_response_threshold
    while poll_timeout > 0.0:

        rq_queue = _get_rq_queue(pmnc.request.parameters["auth_tokens"]["source_cage"])

        request_id_request = rq_queue.pop(poll_timeout)
        if request_id_request is None:
            return None
        request_id, request = request_id_request

        # see if the original caller is still waiting for the request,
        # otherwise the request is silently discarded

        with _rs_queues_lock:
            if request_id in _rs_queues:
                return request

        poll_timeout = pmnc.request.remain - _poll_response_threshold

###############################################################################
# other cages call this method to submit response for a request they processed,
# request id's are unique and random, anyone who knows the id of the request
# can submit the response for it

def post(request_id: str, response):

    with _rs_queues_lock:
        rs_queue = _rs_queues.get(request_id)

    # if the original caller is not waiting for the request any more, an exception
    # is thrown so that the target cage knows at least there was a problem

    if rs_queue is not None:
        rs_queue.push(response)
    else:
        raise Exception("the request is no longer pending for response")

###############################################################################

def self_test():

    from pmnc.request import fake_request
    from expected import expected
    from pmnc.threads import HeavyThread
    from time import time, sleep

    ###################################

    def test_get_rq_queue():

        q1 = _get_rq_queue("foo")
        assert q1 is _get_rq_queue("foo")
        q2 = _get_rq_queue("bar")
        assert q2 is _get_rq_queue("bar")
        assert q1 is not q2

        q1.push("foo")
        assert q2.pop(0.1) is None
        assert q1.pop(0.1) == "foo"

        q2.push("bar")
        assert q1.pop(0.1) is None
        assert q2.pop(0.1) == "bar"

    test_get_rq_queue()

    ###################################

    def test_execute_timeout():

        fake_request(1.0)

        with expected(Exception("request deadline waiting for response")):
            pmnc.__getattr__(__name__).execute_reverse("bad_cage", "module", "method", (), {})

    test_execute_timeout()

    ###################################

    def test_execute_success():

        fake_request(10.0)

        request_id = None
        response = None

        def caller(*args, **kwargs):
            fake_request(6.0)
            nonlocal request_id, response
            request_id = pmnc.request.unique_id
            pmnc.request.parameters["AAA"] = "BBB"
            pmnc.request.describe("my request")
            response = pmnc.__getattr__(__name__).execute_reverse("good_cage", "module", "method", args, kwargs)

        assert "good_cage" not in _rq_queues

        th = HeavyThread(target = caller, args = (1, "foo"), kwargs = { "biz": "baz" })
        th.start()
        try:

            sleep(2.0)

            assert "good_cage" in _rq_queues
            assert request_id in _rs_queues

            req_id, req = _rq_queues["good_cage"].pop()

            assert req_id == request_id
            assert abs(req["request"].pop("deadline") - time() - 4.0) < 1.0

            assert req == dict \
            (
                source_cage = __cage__,
                target_cage = "good_cage",
                module = "module",
                method = "method",
                args = (1, "foo"),
                kwargs = { "biz": "baz" },
                request = dict(protocol = pmnc.request.protocol,
                               interface = pmnc.request.interface,
                               unique_id = request_id,
                               description = "my request",
                               parameters = dict(auth_tokens = {}, AAA = "BBB")),
            )

            _rs_queues[request_id].push({ "result": "RESULT" })

            sleep(2.0)

            assert "good_cage" in _rq_queues
            assert request_id not in _rs_queues

        finally:
            th.stop()

        assert response == "RESULT"

        assert "good_cage" in _rq_queues
        assert request_id not in _rs_queues

    test_execute_success()

    ###################################

    def test_poll():

        fake_request(5.0)
        pmnc.request.parameters["auth_tokens"]["source_cage"] = "source_cage"
        assert pmnc.__getattr__(__name__).poll() is None
        assert abs(pmnc.request.remain - _poll_response_threshold) < 1.0

        fake_request(5.0)
        pmnc.request.parameters["auth_tokens"]["source_cage"] = "source_cage"
        _get_rq_queue("source_cage").push(("RQ-123", "request1"))
        assert pmnc.__getattr__(__name__).poll() is None
        assert abs(pmnc.request.remain - _poll_response_threshold) < 1.0

        fake_request(5.0)
        pmnc.request.parameters["auth_tokens"]["source_cage"] = "source_cage"
        _get_rq_queue("source_cage").push(("RQ-123", "request1"))
        _get_rq_queue("source_cage").push(("RQ-456", "request2"))
        _rs_queues["RQ-456"] = "whatever"
        assert pmnc.__getattr__(__name__).poll() == "request2"
        assert abs(pmnc.request.remain - 5.0) < 1.0

    test_poll()

    ###################################

    def test_post():

        with expected(Exception("the request is no longer pending for response")):
            pmnc.__getattr__(__name__).post("RQ-ABC", "RESULT")

        rs_queue = InterlockedQueue()
        _rs_queues["RQ-ABC"] = rs_queue

        pmnc.__getattr__(__name__).post("RQ-ABC", "RESULT")

        assert rs_queue.pop() == "RESULT"

    test_post()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF