#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# Request class. Implements a logical notion of a request being executed within
# a cage. Any execution within Pythomnic is always bound to some request, which
# is accessible by pmnc.request. Most importantly, request specifies a deadline
# by which the execution must complete. After the deadline has passed, execution
# of a request will be aborted by Pythomnic at its earliest convenience.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "Request", "InfiniteRequest", "fake_request" ]

################################################################################

import os; from os import urandom
import binascii; from binascii import b2a_hex
import time; from time import time, strftime
import threading; from threading import Event, Lock, current_thread
import copy; from copy import deepcopy
import datetime; from datetime import datetime

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import typecheck, optional
import comparable_mixin; from comparable_mixin import ComparableMixin
import shared_lock; from shared_lock import SharedLock
import interlocked_queue; from interlocked_queue import InterlockedQueue

################################################################################

class Request(ComparableMixin):

    @typecheck
    def __init__(self, *,
                 timeout: optional(float) = None,
                 interface: optional(str) = None,
                 protocol: optional(str) = None,
                 parameters: optional(dict) = None,
                 description: optional(str) = None):

        # infinite "requests" can only be anonymous, "normal" requests
        # having a deadline must be assigned an interface and protocol

        self._start = time()
        if timeout is not None:
            self._deadline = self._start + timeout
            assert interface is not None and protocol is not None, \
                   "request with deadline from unspecified interface/protocol"
        else:
            self._deadline = None
            assert interface is None and protocol is None, \
                   "infinite request from specific interface/protocol"

        self._interface, self._protocol = interface, protocol
        self._parameters = parameters or {}

        request_time = strftime("%Y%m%d%H%M%S")
        random_id = b2a_hex(urandom(6)).decode("ascii").upper()
        self._unique_id = "RQ-{0:s}-{1:s}".format(request_time, random_id)
        self._description = description

    ###################################

    # each request has a printable description which appears
    # in every log message written on its behalf

    @typecheck
    def describe(self, s: str):
        self._description = s

    def _rdescription(self):
        if not self.infinite:
            description = " ({0:s})".format(self._description) \
                          if self._description is not None else ""
            remain = self.remain
            deadline = "+{0:.01f}s".format(remain) if remain > 0.0 else \
                       "-{0:.01f}s".format(self.expired_for)
            return "RQ-{0:s}{1:s} via {2:s} {3:s}".\
                   format(self._unique_id[-4:], description,
                          self._interface, deadline)

    description = property(lambda self: self._rdescription())

    ###################################

    # make requests comparable with earliest-deadline-first policy

    def __lt__(self, other):
        if not self.infinite:
            return other.infinite or self._deadline < other._deadline
        else:
            return False

    def __eq__(self, other):
        if not self.infinite:
            return self._deadline == other._deadline
        else:
            return other.infinite

    ###################################

    # timing-related properties

    @typecheck
    def expires_in(self, timeout: float) -> bool:
        return not self.infinite and time() + timeout >= self._deadline

    infinite = property(lambda self: self._deadline is None)
    expired = property(lambda self: self.expires_in(0.0))
    elapsed = property(lambda self: time() - self._start)

    def _rremain(self):
        if self.infinite:
            raise Exception("infinite request has no remain")
        return max(0.0, self._deadline - time())
    remain = property(lambda self: self._rremain())

    def _rexpired_for(self):
        if self.infinite:
            raise Exception("infinite request never expires")
        if not self.expired:
            raise Exception("request has not expired yet")
        return time() - self._deadline
    expired_for = property(lambda self: self._rexpired_for())

    ###################################

    interface = property(lambda self: self._interface)
    parameters = property(lambda self: self._parameters)
    unique_id = property(lambda self: self._unique_id)

    @typecheck
    def _wprotocol(self, protocol: str):
        assert self._protocol is not None, "cannot override unspecified protocol"
        self._protocol = protocol
    protocol = property(lambda self: self._protocol, _wprotocol)

    ###################################

    # this class member may be set once at startup to indicate
    # that some module's self-test is currently being executed

    _self_test = None
    self_test = property(lambda self: self._self_test)

    ###################################

    # used to (de)serialize the request for RPC and retries

    def to_dict(self):
        assert not self.infinite, "cannot serialize infinite request"
        return dict(unique_id = self._unique_id,
                    deadline = self._deadline, # note that deadline is kept rather than timeout
                    interface = self._interface,
                    protocol = self._protocol,
                    parameters = { k: deepcopy(v) for k, v in self._parameters.items()
                                   if isinstance(k, str) and not k.startswith("_") },
                    description = self._description)

    @staticmethod
    def from_dict(d, *, timeout = None):
        if "deadline" in d:
            derived_timeout = max(0.0, d["deadline"] - time())
            timeout = min(timeout or derived_timeout, derived_timeout)
        assert timeout is not None
        result = Request(timeout = timeout, interface = d["interface"],
                         protocol = d["protocol"], parameters = d["parameters"],
                         description = d.get("description")) # description may be absent
        result._unique_id = d["unique_id"]
        return result

    # this method is used to create identical copies of a request when
    # starting multiple parallel control flows on behalf of this request

    def clone(self):
        return self.from_dict(self.to_dict())

    ###################################

    # waits for an event until this request expires
    # (a heavy thread would wait forever)

    @typecheck
    def wait(self, event: Event) -> bool: # respects wall-time timeout, see issue9892
        if self.infinite:
            event.wait()
            return True
        remain = self.remain # there is no special handling for case remain == 0.0, because that
        while remain > 0.0:  # would mean request deadline hence performance is no longer an issue
            event.wait(remain)
            if event.is_set():
                return True
            remain = self.remain
        else:
            return False

    ###################################

    # waits for an exclusive lock until this request expires
    # (a heavy thread would wait forever)

    @typecheck
    def acquire(self, shared_lock: SharedLock) -> bool: # respects wall-time timeout, see issue9892
        if self.infinite:
            return shared_lock.acquire()
        remain = self.remain # there is no special handling for case remain == 0.0, because that
        while remain > 0.0:  # would mean request deadline hence performance is no longer an issue
            if shared_lock.acquire(remain):
                return True
            remain = self.remain
        else:
            return False

    ###################################

    # waits for a shared lock until this request expires
    # (a heavy thread would wait forever)

    @typecheck
    def acquire_shared(self, shared_lock: SharedLock) -> bool: # respects wall-time timeout, see issue9892
        if self.infinite:
            return shared_lock.acquire_shared()
        remain = self.remain # there is no special handling for case remain == 0.0, because that
        while remain > 0.0:  # would mean request deadline hence performance is no longer an issue
            if shared_lock.acquire_shared(remain):
                return True
            remain = self.remain
        else:
            return False

    ###################################

    # waits for a shared lock until this request expires but ignores
    # the fact of expiration if the lock is immediately available

    @typecheck
    def acquire_shared_fast(self, shared_lock: SharedLock) -> bool: # respects wall-time timeout, see issue9892
        if shared_lock.acquire_shared(0.0): # shared lock accepts zero timeout
            return True
        return self.acquire_shared(shared_lock)

    ###################################

    # waits for an item to appear in a queue until this request expires
    # (a heavy thread would wait forever)

    @typecheck
    def pop(self, queue: InterlockedQueue): # respects wall-time timeout, see issue9892
        if self.infinite:
            return queue.pop()
        remain = self.remain # there is no special handling for case remain == 0.0, because that
        while remain > 0.0:  # would mean request deadline hence performance is no longer an issue
            result = queue.pop(remain)
            if result is not None:
                return result
            remain = self.remain
        else:
            return None

################################################################################
# this obvious subclass initiates the request as infinite

class InfiniteRequest(Request):

    def __init__(self):
        Request.__init__(self)

################################################################################
# this method attaches a fake request to the current thread

@typecheck
def fake_request(timeout: optional(float) = None,
                 interface: optional(str) = "__fake__") -> Request:

    if timeout is not None:
        request = Request(timeout = timeout, interface = interface, protocol = "n/a",
                          parameters = dict(auth_tokens = {}))
    else:
        request = InfiniteRequest()

    current_thread()._request = request
    return request

################################################################################

if __name__ == "__main__":

    print("self-testing module request.py:")

    from time import sleep
    from expected import expected
    from typecheck import by_regex, InputParameterError
    from threading import Thread

    ###################################

    r = Request()
    assert not r.expired
    assert not r.expires_in(86400.0)
    assert r.elapsed >= 0.0
    assert r.interface is None
    assert r.protocol is None
    assert r.parameters == {}
    assert r.unique_id.startswith("RQ-20")
    assert r._description is None
    with expected(Exception("infinite request never expires")):
        r.expired_for
    assert not r.self_test

    ###################################

    r = Request(timeout = 0.5, interface = "test", protocol = "tcp",
                parameters = { "foo": "bar", "self-test": "some_module" },
                description = "some request")

    assert not r.expired
    assert r.expires_in(1.0)
    assert r.elapsed >= 0.0
    assert r.interface == "test"
    assert r.protocol == "tcp"
    assert r.parameters == { "foo": "bar", "self-test": "some_module" }
    assert r.unique_id.startswith("RQ-20")
    assert r._description == "some request"
    with expected(Exception("request has not expired yet")):
        r.expired_for

    r2 = r.clone()
    assert r2 is not r
    assert not r2.expired
    assert r2.expires_in(1.0)
    assert abs(r2.elapsed - r.elapsed) < 0.1
    assert r2.interface == "test"
    assert r2.protocol == "tcp"
    assert r2.parameters is not r.parameters and r2.parameters == { "foo": "bar", "self-test": "some_module" }
    assert r2.unique_id == r.unique_id
    assert r2.description == r.description
    with expected(Exception("request has not expired yet")):
        r2.expired_for

    sleep(1.1)

    assert r.expired and r2.expired
    assert r.elapsed >= 1.0 and r2.elapsed >= 1.0 and abs(r2.elapsed - r.elapsed) < 0.1
    assert r.expired_for > 0.0

    ###################################

    r1 = Request(timeout = 1.0, interface = "test", protocol = "test")
    assert not r1.expires_in(0.5)

    sleep(0.6)
    assert r1.expires_in(0.5)

    r2 = r1.clone()
    assert r2.expires_in(0.5)

    ###################################

    auth_tokens = {}

    r = Request(timeout = 6.0, interface = "test", protocol = "tcp",
                parameters = { "foo": "bar", "auth_tokens": auth_tokens })
    assert not r.expires_in(5.5)

    d = r.to_dict()
    assert d == { "deadline": r._deadline,
                  "unique_id": r.unique_id,
                  "interface": "test",
                  "protocol": "tcp",
                  "parameters": { "foo": "bar", "auth_tokens": auth_tokens },
                  "description": None }

    auth_tokens["foo"] = "bar"
    assert "foo" not in d["parameters"]["auth_tokens"] # the deepcopy prevents aliasing

    r1 = r.from_dict(d)
    assert r1.unique_id == r.unique_id
    assert r1.interface == "test"
    assert r1.protocol == "tcp"
    assert r1.parameters == { "foo": "bar", "auth_tokens": {} }
    assert not r1.expires_in(5.5)
    assert r1.expires_in(6.5)
    assert r1.description == r.description

    r2 = r.from_dict(d, timeout = 1.0)
    assert not r2.expires_in(0.5)
    assert r2.expires_in(1.5)

    r3 = r.from_dict(d, timeout = 10.0)
    assert not r3.expires_in(4.0)
    assert r3.expires_in(6.5)

    del d["deadline"]

    r4 = r.from_dict(d, timeout = 10.0)
    assert not r4.expires_in(9.5)
    assert r4.expires_in(10.5)

    # parameters with names starting with underscore
    # are not serialized, same is true about non-str keys

    r = Request(timeout = 1.0, interface = "foo", protocol = "bar",
                parameters = { "_x": "whatever", 1: 2 })

    rd = r.description
    r.describe("Some request")

    d = r.to_dict()
    assert d == { "interface": "foo",
                  "protocol": "bar",
                  "deadline": r._deadline,
                  "parameters": {},
                  "unique_id": r.unique_id,
                  "description": "Some request" }

    # description may be absent (backwards compatibility)

    del d["description"]
    assert Request.from_dict(d).description == rd

    ###################################

    with expected(AssertionError("infinite request from specific interface/protocol")):
        Request(interface = "test")

    with expected(AssertionError("infinite request from specific interface/protocol")):
        Request(protocol = "tcp")

    with expected(AssertionError("request with deadline from unspecified interface/protocol")):
        Request(timeout = 0.1)

    with expected(AssertionError("request with deadline from unspecified interface/protocol")):
        Request(timeout = 0.1, interface = "test")

    with expected(AssertionError("request with deadline from unspecified interface/protocol")):
        Request(timeout = 0.1, protocol = "tcp")

    with expected(AssertionError("cannot override unspecified protocol")):
        Request().protocol = "tcp"

    r = Request(timeout = 1.0, interface = "test", protocol = "tcp")
    r.protocol = "http"
    assert r.protocol == "http"

    ###################################

    e = Event()

    r = Request(timeout = 0.1, interface = "test", protocol = "test")
    before = time()
    assert not r.wait(e) # waiting for event times out
    after = time()
    assert after - before >= 0.1
    assert r.expired

    e.set()

    r = Request(timeout = 0.1, interface = "test", protocol = "test")
    before = time()
    assert r.wait(e) # waiting gor event succeeds
    after = time()
    assert after - before < 0.01
    assert not r.expired

    sleep(0.2)
    assert r.expired

    before = time()
    assert not r.wait(e) # waiting for event fails immediately
    after = time()
    assert after - before < 0.01

    ###################################

    sl = SharedLock()

    r = Request(timeout = 0.1, interface = "test", protocol = "test")
    before = time()
    assert r.acquire(sl) # acquiring succeeds
    after = time()
    assert after - before < 0.01
    assert not r.expired
    sl.release()

    before = time()
    assert r.acquire_shared(sl) # shared acquiring succeeds
    after = time()
    assert after - before < 0.01
    assert not r.expired
    sl.release_shared()

    sleep(0.2)
    assert r.expired

    assert not r.acquire(sl) # acquiring fails
    assert not r.acquire_shared(sl) # shared acquiring fails

    # but

    assert r.acquire_shared_fast(sl) # fast shared acquiring succeeds

    ###################################

    sl = SharedLock()

    def lock(sl):
        e = Event()
        def _lock(sl):
            sl.acquire(); e.set(); sleep(3.0); sl.release()
        Thread(target = _lock, args = (sl, )).start()
        e.wait()

    lock(sl) # make sure the lock can't be acquired

    r = Request(timeout = 0.1, interface = "test", protocol = "test")
    before = time()
    assert not r.acquire(sl) # acquiring times out
    after = time()
    assert after - before >= 0.1
    assert r.expired

    r = Request(timeout = 0.1, interface = "test", protocol = "test")
    before = time()
    assert not r.acquire_shared(sl) # shared acquiring times out
    after = time()
    assert after - before >= 0.1
    assert r.expired

    ###################################

    ilq = InterlockedQueue()

    ilq.push(1)

    r = Request(timeout = 0.1, interface = "test", protocol = "test")
    before = time()
    assert r.pop(ilq) == 1 # popping succeeds
    after = time()
    assert after - before < 0.01
    assert not r.expired

    r = Request(timeout = 0.1, interface = "test", protocol = "test")
    before = time()
    assert r.pop(ilq) is None # popping times out
    after = time()
    assert after - before >= 0.1
    assert r.expired

    ilq.push(2)

    before = time()
    assert r.pop(ilq) is None # popping fails immediately
    after = time()
    assert after - before < 0.01

    ###################################

    assert not Request().self_test
    Request._self_test = "foo"
    assert Request().self_test == "foo"

    ###################################

    assert InfiniteRequest().infinite

    ###################################

    r = fake_request(1.0)
    assert r.parameters["auth_tokens"] == {}
    assert r is current_thread()._request
    assert not r.expired
    sleep(1.1)
    assert r.expired

    ###################################

    r = fake_request(timeout = 1.0, interface = "test")
    assert r.interface == "test"
    assert not r.expired
    sleep(1.1)
    assert r.expired

    ###################################

    r = fake_request()
    assert r is current_thread()._request
    assert isinstance(r, InfiniteRequest)

    ###################################

    r = Request(timeout = 1.0, interface = "test", protocol = "n/a")
    assert by_regex("^RQ-[0-9A-F]{4} via test (?:\\+1\\.0|\\+0\\.[1-9])s$")(r.description)

    r = Request(timeout = 1.0, interface = "test", protocol = "n/a", description = "some request")
    assert by_regex("^RQ-[0-9A-F]{4} \\(some request\\) via test (?:\\+1\\.0|\\+0\\.[1-9])s$")(r.description)

    r.describe("some better request")
    assert by_regex("^RQ-[0-9A-F]{4} \\(some better request\\) via test (?:\\+1\\.0|\\+0\\.[1-9])s$")(r.description)

    sleep(1.5)
    assert by_regex("^RQ-[0-9A-F]{4} \\(some better request\\) via test (?:-0\\.[5-9])s$")(r.description)

    ###################################

    print("ok")

################################################################################
# EOF
