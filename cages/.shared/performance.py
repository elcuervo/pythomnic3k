#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module collects performance measurements from all the other modules,
# which are posting to it key-value pairs using:
#
# pmnc.performance.event(key)          # for fact-like events, such as accepted requests
# pmnc.performance.sample(key, value)  # for numeric readings such as request pending time
# with pmnc.performance.timing(key):   # for measuring the duration of some process in ms
#    ...
#
# There also is a utility method which registers pending time
# and processing time for a request being processed:
#
# with pmnc.performance.request_processing():
#     ... process request ...
#
# Technically, any kind of performance-related information could be collected,
# but it is also has to be displayed in some way, therefore it is currently exactly
# of two kinds - rates (events per second) and durations (how long something took).
# It is also restricted to readings collected by interfaces and resources.
# Again, you can send anything from your application modules, but it will
# not be displayed or otherwise reported, so there is no point. On the other
# hand, readings from interfaces and resources get extracted and displayed by
# on a single web page by the special HTTP interface "performance".
#
# The collected readings are passed through 10 sec averaging, then through 60 sec
# averaging, and the resulting statistics for the last hour is put to a structure
# from which it is extracted by the displaying web interface (interface_performance.py).
#
# Interfaces report the following readings:
#
# interface.foo.request_rate - accepted requests/sec
# interface.foo.pending_time - how long a request waited in queue for execution
# interface.foo.processing_time - request processing time when actually executed
# interface.foo.processing_time.success - same as previous, only successes
# interface.foo.processing_time.failure - same as previous, only failures
# interface.foo.response_time - request handling time with protocol overhead
# interface.foo.response_time.success - same as previous, only successes
# interface.foo.response_time.failure - same as previous, only failures
# interface.foo.response_rate - delivered responses/sec
# interface.foo.response_rate.success - same as previous, only successes
# interface.foo.response_rate.failure - same as previous, only failures
#
# Resources report the following readings:
#
# resource.bar.pending_time - how long a resource waited in queue for execution
# resource.bar.processing_time - resource processing time when actually executed
# resource.bar.processing_time.success - same as previous, only successes
# resource.bar.processing_time.failure - same as previous, only failures
# resource.bar.transaction_rate - resource transaction participations/sec
# resource.bar.transaction_rate.success - same as previous, only successes
# resource.bar.transaction_rate.failure - same as previous, only failures
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "start", "stop", "event", "sample", "timing", "extract", "request_processing" ]
__reloadable__ = False

################################################################################

import time; from time import time
import threading; from threading import current_thread, Lock
import math; from math import log10

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, by_regex, optional
import interlocked_queue; from interlocked_queue import InterlockedQueue
import perf_info; from perf_info import get_working_set_size, get_cpu_times
import pmnc.samplers; from pmnc.samplers import RawSampler, RateSampler
import pmnc.threads; from pmnc.threads import HeavyThread

###############################################################################

# module-level state => not reloadable

_perf_thread = None
_perf_queue = InterlockedQueue()

_perf_lock = Lock()
_perf_dump_60s = []
_perf_dump_10s = [None] * 6
_perf_stats = {}

###############################################################################

def _normalize_time(ms: float) -> int: # takes time in milliseconds, returns 0-39

    if ms <= 10.0:
        return 0
    elif ms >= 100000.0:
        return 39
    else:
        return min(39, max(0, (int(log10(ms / 1000.0) * 16) + 20)))

###############################################################################

def _normalize_rate(rate: float) -> int: # takes rate in reqs/sec, returns 0-39

    if rate <= 0.1:
        return 0
    elif rate >= 1000.0:
        return 39
    else:
        return min(39, max(0, (int(log10(rate / 10.0) * 16) + 20)))

###############################################################################

class Sampler: # this class contains a collection of named samplers

    @typecheck
    def __init__(self, period: int):
        self._period = period
        self._samplers = {}

    def _get_sampler(self, key: str, cls: type):
        sampler = self._samplers.get(key)
        if not sampler:
            sampler = cls(float(self._period))
            self._samplers[key] = sampler
        if type(sampler) is not cls: # can't use isinstance b/c RateSampler is a subclass of RawSampler
            raise Exception("performance key \"{0:s}\" has already been allocated "
                            "as {1:s}".format(key, sampler.__class__.__name__))
        return sampler

    # this method is called whenever the instance has completed collecting
    # and returns the summary of the collected statisitics used for display

    def dump(self) -> dict:
        pass # abstract

###############################################################################

valid_time_key = by_regex("^(interface|resource)\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+_time(\\.failure|\\.success)?$")
valid_rate_key = by_regex("^(interface|resource)\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+_rate(\\.failure|\\.success)?$")

###############################################################################

class CurrentSampler(Sampler):

    @typecheck
    def tick(self, key: valid_rate_key):
        sampler = self._get_sampler(key, RateSampler)
        sampler.tick()

    @typecheck
    def sample(self, key: valid_time_key, value: int):
        sampler = self._get_sampler(key, RawSampler)
        sampler += value

    def dump(self) -> dict:
        d = {}
        for k, v in self._samplers.items():
            norm = valid_time_key(k) and _normalize_time or _normalize_rate
            avg, dev = v.avg, v.dev
            d[k] = (norm(avg - dev), norm(avg + dev))
        return d

###############################################################################

class CumulativeSampler(Sampler):

    @typecheck
    def _sample(self, key: str, value: int):
        sampler = self._get_sampler(key, RawSampler)
        sampler += value

    def collect(self, sampler):
        for k, v in sampler._samplers.items():
            self._sample("{0:s}.min".format(k), v.min)
            self._sample("{0:s}.max".format(k), v.max)
            self._sample("{0:s}.avg".format(k), int(v.avg))
            self._sample("{0:s}.dev".format(k), int(v.dev))

    def dump(self) -> dict:
        keys = set(k.rsplit(".", 1)[0] for k in self._samplers.keys())
        d = {}
        for k in keys:
            k_avg = "{0:s}.avg".format(k); avg = self._samplers[k_avg].avg
            k_dev = "{0:s}.dev".format(k); dev = self._samplers[k_dev].max
            norm = valid_time_key(k) and _normalize_time or _normalize_rate
            d[k] = (norm(avg - dev), norm(avg + dev))
        return d

###############################################################################

class TimeSlice: # this class tracks time and indicates a beginning of a new time slice

    @typecheck
    def __init__(self, sec: int):
        self._sec = sec
        self._last = self._curr()

    @typecheck
    def _curr(self) -> int:
        return int(time()) // self._sec

    @typecheck
    def _next(self) -> (bool, int):
        curr = self._curr()
        next = curr > self._last
        if next: self._last = curr
        return next, curr

    next = property(lambda self: self._next())

###############################################################################

def _perf_thread_proc():

    slice10s = TimeSlice(10)

    sampler60s = CumulativeSampler(60)
    sampler10s = CurrentSampler(10)

    while not current_thread().stopped():
        try:

            # drain the queue and append all the data to the current 10s sampler

            perf_info = _perf_queue.pop(1.0) # this causes a delay of up to 1 sec
            while perf_info:
                what, key, value = perf_info
                if what == "event":
                    sampler10s.tick(key)
                    if key.endswith(".success") or key.endswith(".failure"):
                        sampler10s.tick(key[:-8])
                elif what == "sample":
                    sampler10s.sample(key, value)
                    if key.endswith(".success") or key.endswith(".failure"):
                        sampler10s.sample(key[:-8], value)
                perf_info = _perf_queue.pop(0.0)

            # see if another 10s have passed

            next, slice = slice10s.next
            if next: # start of a 10s slice

                with _perf_lock: # update the shared 10s stats
                    completed_slice = slice - 1
                    _perf_dump_10s[completed_slice % 6] = (completed_slice * 10, sampler10s.dump())

                sampler60s.collect(sampler10s)   # append 10s stats to the 60s sampler
                sampler10s = CurrentSampler(10) # create a new empty 10s sampler

                if slice % 6 == 0: # start of a new minute

                    # update the shared 60s stats with the current 60s summary
                    # and reset the shared 10s stats

                    with _perf_lock:
                        completed_minute = slice - 6
                        _perf_dump_60s.append((completed_minute * 10, sampler60s.dump()))
                        if len(_perf_dump_60s) == 60:
                            _perf_dump_60s.pop(0)
                        _perf_dump_10s[:] = [None] * 6

                    sampler60s = CumulativeSampler(60) # create a new empty 60s sampler

                with _perf_lock: # update global statistics
                    wss = get_working_set_size()
                    if wss is not None:
                        _perf_stats.update(wss = wss)
                    cpu_ut_cpu_kt = get_cpu_times()
                    if cpu_ut_cpu_kt is not None:
                        cpu_ut, cpu_kt = cpu_ut_cpu_kt
                        _perf_stats.update(cpu_ut_percent = (cpu_ut - _perf_stats.get("cpu_ut", 0.0)) * 10.0,
                                           cpu_kt_percent = (cpu_kt - _perf_stats.get("cpu_kt", 0.0)) * 10.0,
                                           cpu_ut = cpu_ut, cpu_kt = cpu_kt)

        except:
            pmnc.log.error(exc_string()) # log and ignore

###############################################################################

def start():
    global _perf_thread
    _perf_thread = HeavyThread(target = _perf_thread_proc, name = "performance")
    _perf_thread.start()

###############################################################################

def stop():
    _perf_thread.stop()

###############################################################################

@typecheck
def event(key: valid_rate_key):
    _perf_queue.push(("event", key, None))

###############################################################################

@typecheck
def sample(key: valid_time_key, value: int):
    _perf_queue.push(("sample", key, value))

###############################################################################

class _ElapsedTimeCounter:

    def __init__(self, key):
        self._key = key

    def __enter__(self):
        self._start = time()
        return self

    elapsed_ms = property (lambda self: int((time() - self._start) * 1000))

    def __exit__(self, t, v, tb):
        outcome = "success" if t is None else "failure"
        sample("{0:s}.{1:s}".format(self._key, outcome), self.elapsed_ms)

def timing(key: valid_time_key):
    return _ElapsedTimeCounter(key)

###############################################################################

def extract() -> optional((int, dict, dict)):

    result = {}

    with _perf_lock:

        # establish a reference base time between 60s shared stats from 10s shared stats

        if _perf_dump_60s:
            base_time = _perf_dump_60s[-1][0] + 60
        else:
            for x in _perf_dump_10s:
                if x is not None:
                    base_time = x[0] // 60 * 60
                    break
            else:
                return None # no stats at all

        # copy the 60s shared stats

        for t, d in reversed(_perf_dump_60s):
            time_delta = base_time - t
            assert time_delta % 60 == 0
            minutes_back = time_delta // 60
            if 0 < minutes_back <= 59:
                i = 59 - minutes_back
                for k, v in d.items():
                    result.setdefault(k, ([None] * 59, [None] * 6))[0][i] = v

        # copy the 10s shared stats

        for x in _perf_dump_10s:
            if x is not None:
                t, d = x
                time_delta = t - base_time
                assert time_delta % 10 == 0
                slices_forward = time_delta // 10
                if 0 <= slices_forward < 6:
                    i = slices_forward
                    for k, v in d.items():
                        result.setdefault(k, ([None] * 59, [None] * 6))[1][i] = v

        # copy the global statistics

        stats = _perf_stats.copy()

    return base_time, result, stats

###############################################################################

class _RequestProcessingTracker:

    def __init__(self):
        self._interface_key = "interface.{0:s}".format(pmnc.request.interface)

    def __enter__(self):
        self._start = time()
        pending_ms = int(pmnc.request.elapsed * 1000)
        sample("{0:s}.pending_time".format(self._interface_key), pending_ms)
        pmnc.log.debug("request processing begins")

    def __exit__(self, t, v, tb):
        outcome = "success" if t is None else "failure"
        pmnc.log.debug("request processing ends with {0:s}".format(outcome))
        processing_ms = int((time() - self._start) * 1000)
        sample("{0:s}.processing_time.{1:s}".format(self._interface_key, outcome), processing_ms)

def request_processing():
    return _RequestProcessingTracker()

###############################################################################

def self_test():

    from expected import expected
    from time import sleep
    from typecheck import InputParameterError
    from pmnc.request import fake_request

    ###################################

    def test_normalization():

        assert _normalize_time(0.0) == 0
        assert _normalize_time(10.0) == 0
        assert _normalize_time(100.0) == 4
        assert _normalize_time(316.2) == 12
        assert _normalize_time(1000.0) == 20
        assert _normalize_time(3162.3) == 28
        assert _normalize_time(10000.0) == 36
        assert _normalize_time(100000.0) == 39

        assert _normalize_rate(0.0) == 0
        assert _normalize_rate(0.1) == 0
        assert _normalize_rate(1.0) == 4
        assert _normalize_rate(3.16) == 12
        assert _normalize_rate(10.0) == 20
        assert _normalize_rate(31.63) == 28
        assert _normalize_rate(100.0) == 36
        assert _normalize_rate(1000.0) == 39

        def color_bars(scale, f, units):
            prev_bar = -1
            bars = []
            for i in range(0, 10000):
                mark = i * scale
                bar = f(mark)
                if bar != prev_bar:
                    bars.append(int(mark))
                    prev_bar = bar
            return "darkgreen: {0[0]:d}-{0[7]:d} {1:s}, " \
                   "green: {0[7]:d}-{0[15]:d} {1:s}, " \
                   "olive: {0[15]:d}-{0[23]:d} {1:s}, " \
                   "yellow: {0[23]:d}-{0[31]:d} {1:s}, " \
                   "red: {0[31]:d}-{0[39]:d} {1:s}".format(bars, units)

        pmnc.log.message("time color bars: {0:s}".format(color_bars(10.0, _normalize_time, "ms")))
        pmnc.log.message("rate color bars: {0:s}".format(color_bars(0.1, _normalize_rate, "req/s")))

    test_normalization()

    ###################################

    def test_sampler():

        s = CurrentSampler(10)

        s.tick("interface.foo.request_rate")
        s.sample("resource.bar.processing_time", 1000)

        assert s.dump() == { "interface.foo.request_rate": (0, 0), "resource.bar.processing_time": (20, 20) }
        sleep(1.2)
        assert s.dump() == { "interface.foo.request_rate": (4, 4), "resource.bar.processing_time": (20, 20) }

    test_sampler()

    ###################################

    def test_time_slice():

        # this test is still probabilistic

        ts = TimeSlice(2)
        sleep(1.0)
        n1, t1 = ts.next
        if n1:
            sleep(1.0)
            n2, t2 = ts.next
            assert not n2 and t2 == t1
            sleep(1.0)
            n3, t3 = ts.next
            assert n3 and t3 == t2 + 1
        else:
            sleep(1.0)
            n2, t2 = ts.next
            assert n2 and t2 == t1 + 1
            sleep(1.0)
            n3, t3 = ts.next
            assert not n3 and t3 == t2

    test_time_slice()

    ###################################

    def test_event():

        with expected(InputParameterError("event() has got an incompatible value for key: 123")):
            pmnc.performance.event(123)

        with expected(InputParameterError("event() has got an incompatible value for key: $$$")):
            pmnc.performance.event("$$$")

        with expected(InputParameterError("event() has got an incompatible value for key: foo")):
            pmnc.performance.event("foo")

        pmnc.performance.event("interface.foo.request_rate")

    test_event()

    ###################################

    def test_sample():

        with expected(InputParameterError("sample() has got an incompatible value for key: 123")):
            pmnc.performance.sample(123, 1000)

        with expected(InputParameterError("sample() has got an incompatible value for key: $$$")):
            pmnc.performance.sample("$$$", 1000)

        with expected(InputParameterError("sample() has got an incompatible value for key: bar")):
            pmnc.performance.sample("bar", 1000)

        with expected(InputParameterError("sample() has got an incompatible value for value: value")):
            pmnc.performance.sample("resource.bar.processing_time", "value")

        pmnc.performance.sample("resource.bar.processing_time", 1000)

    test_sample()

    ###################################

    def test_timing():

        with expected(InputParameterError("timing() has got an incompatible value for key: 123")):
            with pmnc.performance.timing(123):
                pass

        with expected(InputParameterError("timing() has got an incompatible value for key: $$$")):
            with pmnc.performance.timing("$$$"):
                pass

        with expected(InputParameterError("timing() has got an incompatible value for key: biz")):
            with pmnc.performance.timing("biz"):
                pass

        with pmnc.performance.timing("interface.biz.processing_time") as t:
            assert t.elapsed_ms < 100
            sleep(1.0)
            assert t.elapsed_ms > 900

        with expected(Exception("foo")):
            with pmnc.performance.timing("interface.biz.processing_time"):
                raise Exception("foo")

    test_timing()

    ###################################

    def test_request_processing():

        fake_request(1.0)

        with pmnc.performance.request_processing():
            pass

        with expected(ZeroDivisionError):
            with pmnc.performance.request_processing():
                1 / 0

    test_request_processing()

    ###################################

    def test_running():

        fake_request(90.0)

        start = time()
        for i in range(901):
            pmnc.performance.event("resource.baz.response_rate.success")
            pmnc.performance.sample("resource.baz.response_time.success", 1)
            sleep(0.1)
            if i % 10 == 0:
                pmnc.log("wait {0:d}/90".format(i // 10))

        pmnc.performance.extract()

    test_running()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
