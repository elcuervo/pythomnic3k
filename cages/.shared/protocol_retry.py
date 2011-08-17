#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module implements the retried call facility - asynchronous RPC
# with reliable messaging semantics.
#
# Application code initiates a retried call using
#
# retry_id = pmnc("target_cage", queue = "retry").module.method() # to call other cage
# retry_id = pmnc(queue = "retry").module.method() # to call this cage
#
# or in transactions with other resources
#
# xa = pmnc.transaction.create()
# xa.pmnc("target_cage", queue = "retry").module.method()
# xa.jms.send("message text", JMSCorrelationID = "foo")
# retry_id, jms_message_id = xa.execute()
#
# such calls put the call information to persistent storage and return
# immediately with some globally unique random id for the new call.
# Later on attempts to execute the target method are performed.
#
# By default there is just one retry queue configured, with name
# exactly "retry", but you may have as many as you like, by configuring
# and starting more interfaces of "retry" protocol and using the name
# of the queue identical to the name of the interface:
#
# $ cp .shared/config_interface_retry.py cage/config_interface_retry_queue.py
# pmnc("target_cage", queue = "retry_queue").module.method()
#
# Each attempt to execute the target method is done in TPM-like manner:
#
# 1. A message with a wrapped call in it is loaded from the persistent state
#    in a transaction, and the transaction stays uncommitted.
# 2. An attempt to execute the target method is performed.
# 3. If it returns successfully, the transaction is committed and that's it.
# 4. If it throws any exception, the transaction is rolled back, the message
#    remains on storage and its execution will be retried after a while.
#
# Notes:
#
# 1. Attempts are infinite*, the called method must occasionally succeed
#    or it will be retried forever.
#
# *) The calling code can optionally specify absolute expiration time
#    after which the call will be dropped and no longer retried:
#    pmnc(queue = "retry", expires_in = 3600.0).module.method()
#
# 2. For a retried call to another cage, there is an additional step (also
#    retried infinitely) - transmission of the call to the target cage.
#    When the message containing the call is delivered to the target cage,
#    it is immediately put to persistent storage there and from that moment
#    on to retry the call locally is entirely the responsibility of the
#    target cage.
#
# 3. The calling code can specify a unique id of its own, to prevent
#    duplicate execution, for example
#    retry_id1 = pmnc(queue = "retry", id = "transfer/123").module.method()
#    retry_id2 = pmnc(queue = "retry", id = "transfer/123").module.method()
#    then retry_id1 and retry_id2 are different but only one of them
#    will be actually executed.
#
# 4. Retried calls have *no* FIFO semantics, they *can* arrive in any
#    order, especially after (independent) retry attempts.
#
# 5. If a retried call is initiated from within another retried call,
#    and the caller provides a new unique id, that id is decorated
#    with the unique id of the executed retried call being executed,
#    this is useful to initiate named retried processing steps, for example:
#
#    ... initial payment acceptance with external unique id ...
#    pmnc(queue = "retry").provider.process_payment(id = payment_id)
#                                                   ^^^^^^^^^^^^^^^
#    ... later in provider.py constant step names are used ...
#    def process_payment():
#        pmnc(queue = "retry").provider.foo(id = "foo_step")
#        pmnc(queue = "retry").provider.bar(id = "bar_step")
#                                           ^^^^^^^^^^^^^^^
#    def foo(): # this will be executed once for each payment_id
#        ...
#    def bar(): # this will be executed once for each payment_id
#        ...
#
#    Otherwise, should you just have used
#
#    def process_payment():
#        pmnc(queue = "retry").provider.foo()
#        >>> consider a failure here <<<
#        pmnc(queue = "retry").provider.bar()
#
#    and the failure indeed occured, foo step would have been enqueued
#    again upon the next attempt to execute process_payment, which is
#    clearly wrong.
#
# Design goals:
#
# 1. To minimize the chance of failure as much as possible. A message
#    cannot be lost, but it can (if power is cut at the right moment)
#    be duplicated. The chances of this are very small but the methods
#    that are to be called in retried way should better be prepared.
#    For example, a target method can be something like this:
#
#    def method(): # in module.py
#        retry_ctx = pmnc.request.parameters["retry"]
#        retry_id = retry_ctx["id"]
#        attempt = retry_ctx["attempt"]
#        start = retry_ctx["start"]
#        if attempt > 10 or time() > start + 3600:
#            return # do nothing, too late or too many attempts
#        try:
#            ... insert into db (primary_key) values (retry_id) ...
#        except PrimaryKeyViolation:
#            pass # ok, duplicate attempt
#
# 2. To allow the retried calls to accumulate when a cage receives
#    more incoming calls that it can process at the moment, but at
#    the same time to prevent it from becoming overwhelmed with
#    retry attempts. This way retried calls facility can be used
#    as a buffer to handle load peaks or temporary outages that would
#    otherwise have resulted in request failures visible to the clients.
#
# Sample retry interface configuration (config_interface_retry.py):
#
# config = dict \
# (
# protocol = "retry",        # meta
# request_timeout = None,    # meta, optional
# queue_count = 4,           # maximum number of retried calls scheduled concurrently
# keep_order = False,        # True if execution order is to be preserved (implies queue_count = 1)
# retry_interval = 60.0,     # seconds between attempts to execute retried calls
# retention_time = 259200.0, # seconds to keep history of successfully executed calls
# )                          # (once a call gets older than that, it can be duplicated)
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Interface", "Resource" ]

###############################################################################

import pickle; from pickle import dumps as pickle, loads as unpickle
import threading; from threading import current_thread, Lock, Event
import os; from os import urandom
import binascii; from binascii import b2a_hex
import time; from time import time, strftime

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string, trace_string
import typecheck; from typecheck import typecheck, callable, optional, by_regex
import interlocked_queue; from interlocked_queue import InterlockedQueue
import interlocked_counter; from interlocked_counter import InterlockedCounter
import pmnc.resource_pool; from pmnc.resource_pool import TransactionalResource, ResourceError
import pmnc.timeout; from pmnc.timeout import Timeout
import pmnc.threads; from pmnc.threads import HeavyThread
import pmnc.work_sources; from pmnc.work_sources import WorkSources

###############################################################################

valid_retry_id = by_regex("^RT-[0-9]{14}-[0-9A-F]{12}$")
valid_cage_name = by_regex("^(?:[A-Za-z0-9_-]{1,32}|\\.shared)$")
valid_queue_name = by_regex("^[A-Za-z0-9_-]{1,64}$")

###############################################################################
# the following class combines blazingly fast persistent queue
# for small items with slower database option for big ones

class PersistentQueue:

    _q_opts = dict(pagesize = 32768, re_pad = 0x00, q_extentsize = 64)
    _db_opts = dict(pagesize = 32768)

    @typecheck
    def __init__(self, name: str, re_len: int):
        self._re_len = re_len
        self._name = name
        self._queue = pmnc.state.get_queue(name, re_len = re_len, **self._q_opts)
        self._off_db = pmnc.state.get_database(name, **self._db_opts)

    def push(self, txn, value):
        stored_value = pickle(value)
        if len(stored_value) <= self._re_len:
            self._queue.append(stored_value, txn)
        else:
            off_key = b2a_hex(urandom(15))
            self._queue.append(b"." + off_key, txn)
            self._off_db.put(off_key, stored_value, txn)

    def pop(self, txn):
        queue_item = self._queue.consume(txn)
        if queue_item is not None:
            stored_value = queue_item[1]
            if stored_value.startswith(b"."):
                off_key = stored_value[1:31]
                stored_value = self._off_db.get(off_key, None, txn)
                self._off_db.delete(off_key, txn)
            return unpickle(stored_value)

    def peek(self):
        crs = self._queue.cursor()
        try:
            queue_item = crs.first()
            if queue_item is not None:
                stored_value = queue_item[1]
                if stored_value.startswith(b"."):
                    off_key = stored_value[1:31]
                    stored_value = self._off_db.get(off_key, None)
                return unpickle(stored_value)
        finally:
            crs.close()

###############################################################################

class PersistentSet:

    _db_opts = dict(pagesize = 32768)

    @typecheck
    def __init__(self, name: str):
        self._db = pmnc.state.get_database(name, **self._db_opts)

    def add_new(self, txn, key):
        key = pickle(key)
        if self._db.get(key, None, txn) is None:
            self._db.put(key, None, txn)
            return True
        else:
            return False

    def remove(self, txn, key):
        self._db.delete(pickle(key), txn)

###############################################################################
# this utility class is used to keep track of messages that failed
# to move to the slow queue at rollback

class InterlockedSet:

    def __init__(self):
        self._lock, self._set = Lock(), set()

    def add(self, id):
        with self._lock:
            self._set.add(id)

    def remove(self, id):
        with self._lock:
            self._set.remove(id)

    def __contains__(self, id):
        with self._lock:
            return id in self._set

###############################################################################

class Interface: # TPM-like interface for retrying calls

    _large_queue_re_len = 3072 # maximum size of a serialized message to be stored in the queue (not in a separate database)
    _small_queue_re_len = 128  # maximum size of a serialized message id

    @typecheck
    def __init__(self, name: str, *,
                 queue_count: int,
                 keep_order: bool,
                 retry_interval: float,
                 retention_time: float,
                 request_timeout: optional(float) = None,
                 **kwargs):

        self._name = name
        self._queue_count = keep_order and 1 or queue_count # silently adjust queue count
        self._keep_order = keep_order
        self._retry_interval = retry_interval
        self._retention_time = retention_time

        # allocate private persistent queues and databases

        # single raw queue contains whole messages that have just been
        # pushed but not yet filtered nor accepted for execution

        self._raw_queue = PersistentQueue("{0:s}__raw".format(name),
                                          self._large_queue_re_len)

        # filter database contains ids of messages that have
        # already been accepted for execution

        self._filter_db = PersistentSet("{0:s}__filter".format(name))

        # fast queues contain whole messages that have been filtered
        # against the filter database and accepted for execution

        self._fast_queues = \
            tuple(PersistentQueue("{0:s}__fast_{1:02d}".format(name, i),
                                  self._large_queue_re_len)
                  for i in range(self._queue_count))

        # done queues contain just ids of messages executed previously
        # and therefore have smaller space-saving records

        self._done_queues = \
            tuple(PersistentQueue("{0:s}__done_{1:02d}".format(name, i),
                                  self._small_queue_re_len)
                  for i in range(self._queue_count))

        # single slow queue contains whole messages that
        # are waiting for retry

        self._slow_queue = PersistentQueue("{0:s}__slow".format(name),
                                           self._large_queue_re_len)

        # fast queues are used in round-robin fashion using this counter

        self._fast_queue_ctr = InterlockedCounter(self._queue_count)

        # these structures are used to notify interface threads
        # about incoming work and to limit worker thread allocation

        self._raw_work = WorkSources(1, retry_interval)
        self._fast_work = WorkSources(self._queue_count, retry_interval)

        # because moving messages to the slow queue to initiate retry
        # is a failure-prone operation, we keep track of messages that
        # have to go to the slow queue instead of being processed

        self._failed_messages = InterlockedSet()

        # same is true for successfully processed but not yet
        # committed messages, if only to lesser degree,
        # because commit operation is supposedly less fragile

        self._succeeded_messages = InterlockedSet()

        self._request_timeout = request_timeout or \
                                pmnc.config_interfaces.get("request_timeout") # this is now static

        if pmnc.request.self_test == __name__: # self-test
            self._process_request = kwargs["process_request"]

        self._maintenance_period = kwargs.get("maintenance_period", 10.0)

    name = property(lambda self: self._name)

    ###################################

    def start(self):

        # raw thread moves messages from the raw queue to fast queues,
        # consults and updates the filter at the same time

        self._raw_thread = HeavyThread(target = self._raw_thread_proc,
                                       name = "{0:s}:raw".format(self._name))
        self._raw_thread.start()

        # scheduler thread picks messages from fast queues, schedules
        # them for execution and moves them to slow queue upon failure

        self._scheduler_thread = HeavyThread(target = self._scheduler_thread_proc,
                                             name = "{0:s}:sch".format(self._name))
        self._scheduler_thread.start()

        # maintainer thread wakes up periodically to reschedule
        # the previously failed calls and clean up the filter

        self._maintainer_thread = HeavyThread(target = self._maintainer_thread_proc,
                                              name = "{0:s}:mnt".format(self._name))
        self._maintainer_thread.start()

    def cease(self):
        self._maintainer_thread.stop()
        self._scheduler_thread.stop()
        self._raw_thread.stop()

    def stop(self):
        pass

    ###################################
    # the raw thread keeps reading messages from the raw queue,
    # filtering and dispatching them to the fast queues

    def _raw_thread_proc(self):

        while True: # lifetime loop
            try:

                thread_stopped, raw_queue_idx = self._raw_work.begin_work()
                if thread_stopped:
                    break
                if raw_queue_idx is None:
                    continue
                assert raw_queue_idx == 0

                # the way WorkSources object works, begin_work will return in
                # retry_interval even though there is no actual work to do,
                # therefore the code below gets executed periodically even
                # when the cage is idle

                message = None
                try:

                    # pop a message from the raw queue, consult the filter
                    # to see if it is a duplicate, and if not post it
                    # to a fast queue, all in one transaction

                    message, queue_idx = \
                        pmnc.state.implicit_transaction(self._txn_filter_raw_queue)

                    # if a message has been extracted and it wasn't a duplicate
                    # then notify the scheduler thread that one of its fast queues
                    # needs attention

                    if queue_idx is not None:
                        self._fast_work.add_work(queue_idx)
                    elif message is not None:
                        pmnc.log.warning("{0:s} has already been accepted for execution".\
                                         format(message.description))

                finally:
                    self._raw_work.end_work(raw_queue_idx)
                    if message is not None:
                        self._raw_work.add_work(raw_queue_idx) # to repeat polling immediately

            except:
                pmnc.log.error(exc_string()) # log and ignore

    ###################################
    # the scheduler thread keeps scheduling requests for polling
    # fast queues and processing incoming messages

    def _scheduler_thread_proc(self):

        while True: # lifetime loop
            try:

                thread_stopped, queue_idx = self._fast_work.begin_work()
                if thread_stopped:
                    break
                if queue_idx is None:
                    continue
                assert 0 <= queue_idx < self._queue_count

                # the way WorkSources object works, begin_work will return
                # in retry_interval even though there is no actual work to do,
                # therefore the code below gets executed periodically even
                # when the cage is idle

                try:

                    # note that we create and enqueue a request without being
                    # sure that it is going to actually execute a call

                    request = pmnc.interfaces.begin_request(
                                timeout = self._request_timeout,
                                interface = self._name, protocol = "retry",
                                parameters = dict(auth_tokens = dict()),
                                description = "polling fast queue #{0:02d}".format(queue_idx))

                    pmnc.interfaces.enqueue(request, self.wu_poll_fast_queue, (queue_idx, ), {})

                except:                                 # note that if there is no exception,
                    self._fast_work.end_work(queue_idx) # it's the new request's responsibility
                    raise                               # to call end_work

            except:
                pmnc.log.error(exc_string()) # log and ignore

    ###################################
    # the maintainer thread wakes up periodically to reschedule
    # previously failed calls and clean up the filter database

    def _maintainer_thread_proc(self):

        while not current_thread().stopped(self._maintenance_period): # lifetime loop
            try:
                self._retry_failed_calls()
                self._clean_up_filter()
            except:
                pmnc.log.error(exc_string()) # log and ignore

    ###################################
    # this method is periodically called by the maintainer thread
    # to move the head of the slow queue to the fast queues

    def _retry_failed_calls(self):

        while not current_thread().stopped():
            queue_idx = pmnc.state.implicit_transaction(self._txn_pop_slow_queue)
            if queue_idx is None:
                break # while                   # notify the scheduler thread that one
            self._fast_work.add_work(queue_idx) # of its fast queues needs attention

    ###################################
    # this method is periodically called by the raw thread to remove
    # expired heads of the done queues from the filter

    def _clean_up_filter(self):

        total_removed_records = 0

        while not current_thread().stopped():
            removed_records = 0
            for done_queue in self._done_queues:
                if pmnc.state.implicit_transaction(self._txn_pop_done_queue, done_queue):
                    removed_records += 1
            if removed_records > 0:
                total_removed_records += removed_records
            else:
                break # while

        if total_removed_records > 0:
            pmnc.log.debug("removed {0:d} expired filter record(s)".\
                           format(total_removed_records))

    ###################################
    # this method moves one message from the slow queue to the next
    # fast queue, returns the index of the fast queue or None

    def _txn_pop_slow_queue(self, txn): # this method is executed within transaction

        slow_queue_item = self._slow_queue.peek() # except for this call which is outside
        if slow_queue_item is None:
            return None
        failure_timestamp, wrapped_message = slow_queue_item

        # see if the head of the slow queue is old enough to be retried

        if int(time()) < failure_timestamp + self._retry_interval:
            return None

        # move the message from the slow queue to the next fast queue,
        # because peeking was outside of transaction, we need to compare the data

        assert self._slow_queue.pop(txn) == slow_queue_item # comparing tuples of simple types
        message = RetriedCallMessage.unwrap(wrapped_message)

        queue_idx = self._fast_queue_ctr.next()
        fast_queue = self._fast_queues[queue_idx]
        fast_queue.push(txn, wrapped_message)

        pmnc.log.debug("moving {0:s} from slow queue to fast queue "
                       "#{1:02d}".format(message.description, queue_idx))

        return queue_idx

    ###################################
    # this method pops a single message off each done queues and removes
    # popped message ids from the filter if they are old enough to fall
    # behind the retention threshold, returns number of removed messages

    def _txn_pop_done_queue(self, txn, done_queue): # this method is executed within transaction

        done_queue_item = done_queue.peek() # except for this call which is outside
        if done_queue_item is None:
            return None
        success_timestamp, filtered_id = done_queue_item

        # see if the head of this done queue is old enough to be dropped

        if int(time()) < success_timestamp + self._retention_time:
            return None

        # pop the expired record from the queue and remove it from the filter,
        # because peeking was outside of transaction, we need to compare the data

        assert done_queue.pop(txn) == done_queue_item # comparing tuples of simple types
        self._filter_db.remove(txn, filtered_id)

        return filtered_id

    ###################################
    # this method is called by one of the worker threads to extract
    # and process a message from a fast queue

    def wu_poll_fast_queue(self, queue_idx):

        try:

            # see for how long the request was on the execution queue up to this moment
            # and whether it has expired in the meantime, if it did there is no reason
            # to proceed and we silently exit

            if pmnc.request.expired: # no need to report anything, it's a service request
                success = False
                return # goes through finally section below

            poll_queue_again = False
            try:

                # initiate a transaction and pop a message from the fast queue in that transaction

                txn, commit, rollback = pmnc.state.start_transaction(__name__)
                try:
                    txn, message = \
                        pmnc.state.explicit_transaction(__name__,
                            txn, self._txn_pop_fast_queue_success, queue_idx)
                except:
                    rollback(txn)
                    raise

                if message is None: # commit fruitless pop and return
                    commit(txn)
                    success = True
                    return # goes through finally section below

                try:

                    # now we know more about the request, it is still the current
                    # request, we haven't impersonated the retried request yet

                    pmnc.request.describe(message.description)

                    if message.unique_id in self._succeeded_messages: # previously succeeded, not committed

                        poll_queue_again = self._handle_success(commit, rollback, txn, queue_idx, message)

                    elif message.unique_id in self._failed_messages: # previously failed, not moved to the slow queue

                        poll_queue_again = self._handle_failure(commit, rollback, txn, queue_idx, message)

                    else: # previously unseen message

                        try:
                            with pmnc.performance.request_processing():
                                result = message.execute(self._process_request)
                        except:
                            self._failed_messages.add(message.unique_id)
                            pmnc.log.error("enqueued call has failed but will be retried: {0:s}".format(exc_string()))
                            poll_queue_again = self._handle_failure(commit, rollback, txn, queue_idx, message)
                        else:
                            self._succeeded_messages.add(message.unique_id)
                            pmnc.log.debug("enqueued call has returned successfully")
                            poll_queue_again = self._handle_success(commit, rollback, txn, queue_idx, message)

                except:
                    rollback(txn) # in case of an unexpected error, transaction needs to be disposed of
                    raise

            finally:
                self._fast_work.end_work(queue_idx)
                if poll_queue_again:
                    self._fast_work.add_work(queue_idx) # to repeat polling immediately

        except:
            pmnc.log.error(exc_string()) # log and ignore
            success = False
        else:
            success = True
        finally:                                 # the request ends itself
            pmnc.interfaces.end_request(success) # possibly way after deadline

    ###################################

    def _handle_success(self, commit, rollback, txn, queue_idx, message):
        try:
            commit(txn) # commit fruitful pop
        except:
            pmnc.log.error(exc_string()) # log and ignore
            return True # failure to commit, process the same message again immediately
        else:
            pmnc.log.debug("enqueued call has ultimately completed")
            self._succeeded_messages.remove(message.unique_id)
            return True # process the next message in the queue immediately

    ###################################

    def _handle_failure(self, commit, rollback, txn, queue_idx, message):
        try:
            rollback(txn) # rollback fruitful pop
            if not self._keep_order:
                pmnc.state.implicit_transaction(
                    self._txn_pop_fast_queue_failure, queue_idx, message)
            else:
                assert queue_idx == 0
                pmnc.log.debug("leaving enqueued call in fast queue "
                               "#{0:02d} to maintain ordering".format(queue_idx))
        except:
            pmnc.log.error(exc_string())
            return True # failure to rollback or dispose of to the slow queue, process the same message again immediately
        else:
            self._failed_messages.remove(message.unique_id)
            return not self._keep_order # process the next message in the queue if the head of the queue has been disposed of

    ###################################
    # success action is to register the time of successful completion (now)
    # by pushing filtered id of the message to the corresponding done queue

    def _txn_pop_fast_queue_success(self, txn, queue_idx): # this method is executed in a transaction

        fast_queue = self._fast_queues[queue_idx]
        wrapped_message = fast_queue.pop(txn)
        if wrapped_message is None:
            return None
        message = RetriedCallMessage.unwrap(wrapped_message) # got a call to execute

        # register current time as the time of successful execution
        # of the call, and push registration record to the done queue

        success_timestamp = int(time())
        done_queue_item = (success_timestamp, message.filtered_id)
        done_queue = self._done_queues[queue_idx]
        done_queue.push(txn, done_queue_item)

        pmnc.log.debug("moving {0:s} from fast queue #{1:02d} to done queue "
                       "#{1:02d}".format(message.description, queue_idx))

        return message

    ###################################
    # failure action is to register the time of failure (now)
    # and push the re-wrapped message to the slow queue

    def _txn_pop_fast_queue_failure(self, txn, queue_idx, original_message): # this method is executed in a transaction

        fast_queue = self._fast_queues[queue_idx]
        wrapped_message = fast_queue.pop(txn)
        if wrapped_message is None:
            return None
        message = RetriedCallMessage.unwrap(wrapped_message)
        assert message.unique_id == original_message.unique_id # make sure we fail the right message

        # register current time as the time of failure and push
        # registration record to the slow queue

        failure_timestamp = int(time())
        slow_queue_item = (failure_timestamp, message.wrap())
        self._slow_queue.push(txn, slow_queue_item)

        pmnc.log.debug("moving enqueued call from fast queue #{0:02d} "
                       "to slow queue".format(queue_idx))

    ###################################

    def _txn_filter_raw_queue(self, txn): # this method is executed in a transaction

        # pop a message from the raw queue

        wrapped_message = self._raw_queue.pop(txn)
        if wrapped_message is None:
            return None, None # the raw queue is empty
        message = RetriedCallMessage.unwrap(wrapped_message)

        # filter the message against a database of already accepted,
        # and add it to the the filter if it hasn't been there

        if self._filter_db.add_new(txn, message.filtered_id):

            # push the message to the next fast queue

            queue_idx = self._fast_queue_ctr.next()
            fast_queue = self._fast_queues[queue_idx]
            fast_queue.push(txn, wrapped_message)

            pmnc.log.debug("moving {0:s} from raw queue to fast queue #{1:02d}".\
                           format(message.description, queue_idx))

            return message, queue_idx

        else: # duplicate, ignore

            return message, None

    ###################################

    def _process_request(self, module, method, args, kwargs):
        return pmnc.__getattr__(module).__getattr__(method)(*args, **kwargs)

    ###################################
    # this method is used by Resource instances to notify
    # the interface about a retried call just enqueued

    def raw_queue_alert(self):
        self._raw_work.add_work(0)

###############################################################################

class Resource(TransactionalResource): # "retry" resource for enqueueing calls in transactions

    def connect(self):
        TransactionalResource.connect(self)
        self._attrs = []

    def __getattr__(self, name):
        self._attrs.append(name)
        return self

    ###################################

    def begin_transaction(self, *args, **kwargs):
        TransactionalResource.begin_transaction(self, *args, **kwargs)
        self._txn, self._commit, self._rollback = \
            pmnc.state.start_transaction(__name__)

    ###################################

    def __call__(self, *args, **kwargs):

        try:

            # extract module and method names collected in __getattr__

            assert len(self._attrs) == 2, "expected module.method syntax"
            (module, method), self._attrs = self._attrs, []

            # extract and validate call parameters from pmnc(...) arguments

            queue_name = self.resource_kwargs.pop("queue", None)
            assert queue_name is not None, "queue name must be specified"
            assert valid_queue_name(queue_name), "invalid queue name"

            if len(self.resource_args) == 1:
                target_cage = self.resource_args[0]
            elif len(self.resource_args) == 0:
                target_cage = __cage__
            else:
                assert False, "expected xa.pmnc([\"target_cage\", ]queue = \"queue\") syntax"
            assert valid_cage_name(target_cage), "invalid cage name"

            filtered_id = self.resource_kwargs.pop("id", None)
            assert filtered_id is None or isinstance(filtered_id, str), \
                   "retry id must be an instance of str"
            if filtered_id is not None:
                filtered_id = self._decorate_filtered_id(filtered_id)

            expires_in = self.resource_kwargs.pop("expires_in", None)
            assert expires_in is None or isinstance(expires_in, float), \
                   "expiration time must be an instance of float"
            deadline = expires_in is not None and int(time() + expires_in) or None

            assert not self.resource_kwargs, "unexpected options encountered"

            # remote call is reduced to local call to remote_call.transmit

            if target_cage != __cage__:
                module, method, args, kwargs = \
                    "remote_call", "transmit", (target_cage, module, method, args, kwargs), {}

            # the retry interface for such named queue may not have been started,
            # but we still allow the call to be enqueued, it is just that it will
            # not be processed until the interface is enabled

            self._retry_interface = pmnc.interfaces.get_interface(queue_name)

            # wrap up a message and push it to the persistent raw queue

            raw_queue = PersistentQueue("{0:s}__raw".format(queue_name),
                                        Interface._large_queue_re_len)

            request_dict = pmnc.request.to_dict()
            request_dict.pop("deadline") # since it's running asynchronously, it shouldn't
                                         # be inheriting the current request's deadline

            self._message = RetriedCallMessage(module, method, args, kwargs, request_dict,
                                               deadline = deadline, filtered_id = filtered_id)

            self._txn = pmnc.state.explicit_transaction(__name__,
                            self._txn, raw_queue.push, self._message.wrap())[0]

            # the transaction in which the message is pushed remains open

        except:
            ResourceError.rethrow(recoverable = True) # no irreversible changes
        else:
            return self._message.unique_id

        # the returned value is always unique and does not indicate
        # whether the attempt to execute the call is a duplicate,
        # this fact can only be determined later by the raw thread

    ###################################

    def commit(self):
        self._commit(self._txn)
        if self._message:
            pmnc.log.debug("successfully {0:s}".format(self._message.description))
            if self._retry_interface: # notify the interface
                self._retry_interface.raw_queue_alert()

    ###################################

    def rollback(self):
        self._rollback(self._txn)

    ###################################
    # this method combines arbitrary id specified by the caller in
    # pmnc(..., id = "id") with the retry id of the current request

    @staticmethod
    def _decorate_filtered_id(filtered_id):
        retry_context = pmnc.request.parameters.get("retry")
        if retry_context is not None:
            return "{0:s}:{1:s}".format(retry_context["id"], filtered_id)
        else:
            return filtered_id

###############################################################################
# the following class encapsulates the retried call information,
# wrapped for persistent storage, then unwrapped and executed

class RetriedCallMessage:

    @typecheck
    def __init__(self, module: str, method: str, args: tuple, kwargs: dict, request_dict: dict, *,
                 unique_id: optional(valid_retry_id) = None,
                 filtered_id: optional(str) = None,
                 start: optional(int) = None,
                 attempt: optional(int) = None,
                 deadline: optional(int) = None): # global deadline for retry attempts

        self._module = module
        self._method = method
        self._args = args
        self._kwargs = kwargs
        self._request_dict = request_dict

        self._unique_id = unique_id or self._make_unique_id()
        self._filtered_id = filtered_id if filtered_id is not None else self._unique_id

        self._start = start or int(time())
        self._attempt = attempt or 0 # incremented in wrap
        self._deadline = deadline

    ###################################

    unique_id = property(lambda self: self._unique_id)
    filtered_id = property(lambda self: self._filtered_id)
    expired = property(lambda self: self._deadline is not None and time() > self._deadline)
    attempt = property(lambda self: self._attempt)

    def _rdescription(self):
        if not hasattr(self, "_description"):
            if self._module == "remote_call" and self._method == "transmit":
                call_info = "RPC call {0[1]:s}.{0[2]:s} to {0[0]:s}".format(self._args)
            else:
                call_info = "local call {0:s}.{1:s}".format(self._module, self._method)
            self._description = "enqueued {0:s} as RT-{1:s}{2:s}{3:s}".\
                                format(call_info, self._unique_id[-4:],
                                " ({0:s})".format(self._filtered_id) if self._filtered_id != self._unique_id else "",
                                ", att. {0:d}".format(self._attempt) if self._attempt > 0 else "")
        return self._description

    description = property(lambda self: self._rdescription())

    ###################################

    @staticmethod
    def _make_unique_id():
        current_time = strftime("%Y%m%d%H%M%S")
        random_id = b2a_hex(urandom(6)).decode("ascii").upper()
        return "RT-{0:s}-{1:s}".format(current_time, random_id)

    ###################################

    def wrap(self):
        return dict(module = self._module, method = self._method,
                    args = self._args, kwargs = self._kwargs, deadline = self._deadline,
                    filtered_id = self._filtered_id, unique_id = self._unique_id,
                    start = self._start, attempt = self._attempt + 1, # note the increment
                    request_dict = self._request_dict)

    @classmethod
    def unwrap(cls, d):
        return cls(d["module"], d["method"], d["args"], d["kwargs"], d["request_dict"],
                   unique_id = d["unique_id"], filtered_id = d["filtered_id"],
                   start = d["start"], attempt = d["attempt"], deadline = d["deadline"])

    ###################################

    def execute(self, process_request):

        # if the call has expired, exit and let the message be committed

        if self.expired:
            pmnc.log.warning("enqueued call has expired and will not be retried any more")
            return None # the result is ignored except for self-tests

        request = pmnc.request.from_dict(self._request_dict, timeout = pmnc.request.remain)

        pmnc.log.debug("impersonating request {0:s}".format(request.description))
        original_request = current_thread()._request
        current_thread()._request = request
        try:
            # inject retry-specific information into the impersonated request
            pmnc.request.parameters["retry"] = \
                dict(id = self._unique_id, start = self._start,
                     attempt = self._attempt, deadline = self._deadline)
            result = process_request(self._module, self._method, self._args, self._kwargs)
        finally:
            current_thread()._request = original_request

        return (result, ) # the result is ignored except for self-tests

###############################################################################

def self_test():

    from typecheck import InputParameterError
    from expected import expected
    from time import sleep
    from random import randint, normalvariate
    from hashlib import sha1
    from pmnc.request import fake_request
    from pmnc.self_test import active_interface

    ###################################

    test_interface_config = dict \
    (
    protocol = "retry",
    queue_count = 4,
    keep_order = False,
    retry_interval = 60.0,
    retention_time = 259200.0,
    )

    def interface_config(**kwargs):
        result = test_interface_config.copy()
        result.update(kwargs)
        return result

    ###################################

    rus = "\u0410\u0411\u0412\u0413\u0414\u0415\u0401\u0416\u0417\u0418\u0419" \
          "\u041a\u041b\u041c\u041d\u041e\u041f\u0420\u0421\u0422\u0423\u0424" \
          "\u0425\u0426\u0427\u0428\u0429\u042c\u042b\u042a\u042d\u042e\u042f" \
          "\u0430\u0431\u0432\u0433\u0434\u0435\u0451\u0436\u0437\u0438\u0439" \
          "\u043a\u043b\u043c\u043d\u043e\u043f\u0440\u0441\u0442\u0443\u0444" \
          "\u0445\u0446\u0447\u0448\u0449\u044c\u044b\u044a\u044d\u044e\u044f"

    ###################################

    def test_make_ids():

        fake_request(10.0)

        unique_id1 = RetriedCallMessage._make_unique_id()
        unique_id2 = RetriedCallMessage._make_unique_id()
        assert valid_retry_id(unique_id1) and valid_retry_id(unique_id2)
        assert unique_id1 != unique_id2

        assert Resource._decorate_filtered_id("foo") == "foo"
        pmnc.request.parameters["retry"] = { "id": "bar" }
        assert Resource._decorate_filtered_id("foo") == "bar:foo"

    test_make_ids()

    ###################################

    def test_retried_call_message():

        fake_request(10.0)

        rcm1 = RetriedCallMessage("module", "method", (), {}, pmnc.request.to_dict())

        assert valid_retry_id(rcm1.unique_id)
        assert rcm1.filtered_id == rcm1.unique_id
        assert not rcm1.expired
        assert rcm1.attempt == 0
        assert rcm1.description == "enqueued local call module.method as RT-{0:s}".format(rcm1.unique_id[-4:])

        ###############################

        fake_request(10.0)
        retry_id = "RT-20100901171500-0123456789AB"
        now = int(time())

        rcm2 = RetriedCallMessage("remote_call", "transmit", ("cage", "module", "method") , {}, pmnc.request.to_dict(),
                                  unique_id = retry_id, filtered_id = "foo", start = now - 10, attempt = 2, deadline = now + 1)

        assert rcm2.unique_id == retry_id
        assert rcm2.filtered_id == "foo"
        assert not rcm2.expired
        sleep(1.5)
        assert rcm2.expired
        assert rcm2.attempt == 2
        assert rcm2.description == "enqueued RPC call module.method to cage as RT-89AB (foo), att. 2"

        ###############################

        fake_request(10.0)
        retry_id = "RT-20100901173000-0123456789AB"
        now = int(time())
        start_at = now - 10
        deadline_at = now + 1

        rcm3 = RetriedCallMessage("foo", "bar", (1, "2"), { "biz": "baz" }, pmnc.request.to_dict(),
                                  unique_id = retry_id, filtered_id = "foo", start = start_at, attempt = 2, deadline = deadline_at)
        d = rcm3.wrap()
        rcm4 = RetriedCallMessage.unwrap(d)

        assert rcm4._module == rcm3._module
        assert rcm4._method == rcm3._method
        assert rcm4._args == rcm3._args
        assert rcm4._kwargs == rcm3._kwargs
        assert rcm4._request_dict == rcm3._request_dict
        assert rcm4._unique_id == rcm3._unique_id
        assert rcm4._filtered_id == rcm3._filtered_id
        assert rcm4._start == rcm3._start
        assert rcm4._attempt == rcm3._attempt + 1
        assert rcm4._deadline == rcm3._deadline

        r1 = pmnc.request.to_dict()
        fake_request(10.0)
        assert pmnc.request.unique_id != r1["unique_id"]

        def process_request(module, method, args, kwargs):
            return module, method, args, kwargs, pmnc.request.to_dict()

        pmnc.request.describe(rcm4.description)
        module, method, args, kwargs, request_dict = rcm4.execute(process_request)[0]
        assert (module, method, args, kwargs) == ("foo", "bar", (1, "2"), { "biz": "baz" })
        sleep(1.5)
        assert rcm4.execute(process_request) is None

        retry_ctx = request_dict["parameters"].pop("retry")
        assert retry_ctx == dict(attempt = 3, id = retry_id,
                                 start = start_at, deadline = deadline_at) # global expiration deadline
        assert abs(r1.pop("deadline") - request_dict.pop("deadline")) < 0.01 # attempt request deadline
        assert r1 == request_dict

        ###############################

        fake_request(10.0)

        rcm5 = RetriedCallMessage("foo", "bar", (), {}, pmnc.request.to_dict())

        def process_request(module, method, args, kwargs):
            1 / 0

        pmnc.request.describe(rcm5.description)
        with expected(ZeroDivisionError):
            rcm5.execute(process_request)

    test_retried_call_message()

    ###################################

    def test_invalid_resource_use():

        fake_request(10.0)

        xa = pmnc.transaction.create()
        xa.pmnc().biz.baz()
        try:
            xa.execute()
        except ResourceError as e:
            assert e.description == "queue name must be specified"
            assert e.recoverable and e.terminal
        else:
            assert False

        xa = pmnc.transaction.create()
        xa.pmnc("foo", "bar", queue = "queue").biz.baz()
        try:
            xa.execute()
        except ResourceError as e:
            assert e.description == "expected xa.pmnc([\"target_cage\", ]queue = \"queue\") syntax"
            assert e.recoverable and e.terminal
        else:
            assert False

        xa = pmnc.transaction.create()
        xa.pmnc(123, queue = "queue").biz.baz()
        try:
            xa.execute()
        except ResourceError as e:
            assert e.description == "invalid cage name"
            assert e.recoverable and e.terminal
        else:
            assert False

        xa = pmnc.transaction.create()
        xa.pmnc(rus, queue = "queue").biz.baz()
        try:
            xa.execute()
        except ResourceError as e:
            assert e.description == "invalid cage name"
            assert e.recoverable and e.terminal
        else:
            assert False

        xa = pmnc.transaction.create()
        xa.pmnc("cage").biz.baz()
        try:
            xa.execute()
        except ResourceError as e:
            assert e.description == "queue name must be specified"
            assert e.recoverable and e.terminal
        else:
            assert False

        xa = pmnc.transaction.create()
        xa.pmnc("cage", queue = 123).biz.baz()
        try:
            xa.execute()
        except ResourceError as e:
            assert e.description == "invalid queue name"
            assert e.recoverable and e.terminal
        else:
            assert False

        xa = pmnc.transaction.create()
        xa.pmnc("cage", queue = rus).biz.baz()
        try:
            xa.execute()
        except ResourceError as e:
            assert e.description == "invalid queue name"
            assert e.recoverable and e.terminal
        else:
            assert False

        xa = pmnc.transaction.create()
        xa.pmnc("cage", queue = "Aa_9", id = 123).biz.baz()
        try:
            xa.execute()
        except ResourceError as e:
            assert e.description == "retry id must be an instance of str"
            assert e.recoverable and e.terminal
        else:
            assert False

        xa = pmnc.transaction.create()
        xa.pmnc("cage", queue = "foo", expires_in = 123).biz.baz()
        try:
            xa.execute()
        except ResourceError as e:
            assert e.description == "expiration time must be an instance of float"
            assert e.recoverable and e.terminal
        else:
            assert False

        xa = pmnc.transaction.create()
        xa.pmnc("cage", queue = "foo", expires = 123.0).biz.baz()
        try:
            xa.execute()
        except ResourceError as e:
            assert e.description == "unexpected options encountered"
            assert e.recoverable and e.terminal
        else:
            assert False

    test_invalid_resource_use()

    ###################################

    def test_resource_enqueue1():

        fake_request(10.0)
        rq = pmnc.request.to_dict()

        def process_message(module, method, args, kwargs):
            return module, method, args, kwargs, pmnc.request.to_dict()

        # note that the call is enqueued despite the interface not running

        start_at = int(time())

        xa = pmnc.transaction.create()
        xa.pmnc(queue = "retry").foo.bar(1, "2", biz = "baz")
        retry_id = xa.execute()[0]
        assert valid_retry_id(retry_id)

        sleep(1.5)
        fake_request(10.0)

        raw_queue = PersistentQueue("retry__raw", Interface._large_queue_re_len)
        message = RetriedCallMessage.unwrap(pmnc.state.implicit_transaction(raw_queue.pop))

        assert message.unique_id == message.filtered_id == retry_id
        assert not message.expired and message.attempt == 1
        assert message.description == "enqueued local call foo.bar as RT-{0:s}, att. 1".format(retry_id[-4:])
        module, method, args, kwargs, request_dict = message.execute(process_message)[0]
        assert (module, method, args, kwargs) == ("foo", "bar", (1, "2"), { "biz": "baz" })
        retry_ctx = request_dict["parameters"].pop("retry")
        assert retry_ctx["attempt"] == 1
        assert retry_ctx["id"] == retry_id
        assert retry_ctx["start"] - start_at <= 1
        assert retry_ctx["deadline"] is None
        assert 1.0 < abs(rq.pop("deadline") - request_dict.pop("deadline")) < 2.0 # attempt request deadline inherited from the second request
        assert rq == request_dict

    test_resource_enqueue1()

    ###################################

    def test_resource_enqueue2():

        fake_request(10.0)
        rq = pmnc.request.to_dict()

        def process_message(module, method, args, kwargs):
            return module, method, args, kwargs, pmnc.request.to_dict()

        # note that the call is enqueued despite the interface not running

        start_at = int(time())

        xa = pmnc.transaction.create()
        xa.pmnc("target_cage", queue = "queue", id = rus, expires_in = 3.0).biz.baz(2, "1", foo = "bar")
        retry_id = xa.execute()[0]
        assert valid_retry_id(retry_id)

        sleep(1.5)
        fake_request(10.0)
        raw_queue = PersistentQueue("queue__raw", Interface._large_queue_re_len)
        message = RetriedCallMessage.unwrap(pmnc.state.implicit_transaction(raw_queue.pop))

        assert message.unique_id == retry_id
        assert message.filtered_id == rus
        assert not message.expired and message.attempt == 1
        assert message.description == "enqueued RPC call biz.baz to target_cage as RT-{0:s} ({1:s}), att. 1".format(retry_id[-4:], rus)
        module, method, args, kwargs, request_dict = message.execute(process_message)[0]
        assert (module, method, args, kwargs) == ("remote_call", "transmit", ("target_cage", "biz", "baz", (2, "1"), { "foo": "bar" }), {})
        retry_ctx = request_dict["parameters"].pop("retry")
        assert retry_ctx["attempt"] == 1
        assert retry_ctx["id"] == retry_id
        assert retry_ctx["start"] - start_at <= 1
        assert 2 <= retry_ctx["deadline"] - start_at <= 4
        assert 1.0 < abs(rq.pop("deadline") - request_dict.pop("deadline")) < 2.0 # attempt request deadline inherited from the second request
        assert rq == request_dict

        sleep(4.0)
        assert message.execute(process_message) is None

    test_resource_enqueue2()

    ###################################

    def test_start_stop():

        def process_request(module, method, args, kwargs):
            pass

        with active_interface("retry_start_stop", **interface_config(process_request = process_request,
                              retry_interval = 3.0, maintenance_period = 1.0)):
            sleep(4.0)

    test_start_stop()

    ###################################

    def test_immediate_success():

        loopback_queue = InterlockedQueue()

        def process_request(module, method, args, kwargs):
            loopback_queue.push((module, method, args, kwargs, pmnc.request.to_dict()))

        with active_interface("retry_immediate_success", **interface_config(process_request = process_request,
                              request_timeout = 30.0)):

            fake_request(10.0)

            xa = pmnc.transaction.create()
            xa.pmnc(queue = "retry_immediate_success").foo.bar()
            retry_id = xa.execute()[0]

            (module, method, args, kwargs, request_dict) = loopback_queue.pop(3.0)
            assert module == "foo" and method == "bar" and args == () and kwargs == {}
            rq = pmnc.request.from_dict(request_dict)
            assert rq.remain > 20.0
            assert rq.parameters["retry"]["id"] == retry_id
            assert rq.parameters["retry"]["attempt"] == 1

    test_immediate_success()

    ###################################

    def test_one_failure():

        loopback_queue = InterlockedQueue()

        def process_request(module, method, args, kwargs):
            if pmnc.request.parameters["retry"]["attempt"] == 1:
                1 / 0
            else:
                loopback_queue.push(pmnc.request.to_dict())

        with active_interface("retry_one_failure", **interface_config(process_request = process_request,
                              request_timeout = 3.0, retry_interval = 4.0, maintenance_period = 1.0)):

            fake_request(10.0)

            xa = pmnc.transaction.create()
            xa.pmnc(queue = "retry_one_failure").foo.bar()
            retry_id = xa.execute()[0]

            assert loopback_queue.pop(3.0) is None
            rq = pmnc.request.from_dict(loopback_queue.pop(5.0))
            assert loopback_queue.pop(6.0) is None

            assert rq.remain < 3.5
            assert rq.parameters["retry"]["id"] == retry_id
            assert rq.parameters["retry"]["attempt"] == 2

    test_one_failure()

    ###################################

    def test_one_failure_with_order():

        loopback_queue = InterlockedQueue()

        def process_request(module, method, args, kwargs):
            retry_id = pmnc.request.parameters["retry"]["id"]
            attempt = pmnc.state.get(retry_id, 1) # need a separate mechanism to track attempts
            if attempt == 1:
                pmnc.state.set(retry_id, 2)
                1 / 0
            else:
                loopback_queue.push(pmnc.request.to_dict())

        with active_interface("retry_one_failure_with_order", **interface_config(process_request = process_request,
                              keep_order = True, retry_interval = 4.0, maintenance_period = 1.0)):

            fake_request(10.0)

            xa = pmnc.transaction.create()
            xa.pmnc(queue = "retry_one_failure_with_order").foo.bar()
            retry_id = xa.execute()[0]

            assert loopback_queue.pop(3.0) is None
            rq = pmnc.request.from_dict(loopback_queue.pop(3.0))
            assert loopback_queue.pop(6.0) is None

            # note that with order the attempt is always #1,
            # because the message could not be updated in the queue

            assert rq.parameters["retry"]["id"] == retry_id
            assert rq.parameters["retry"]["attempt"] == 1

    test_one_failure_with_order()

    ###################################

    def test_order():

        loopback_queue = InterlockedQueue()

        def process_request(module, method, args, kwargs):
            retry_id = pmnc.request.parameters["retry"]["id"]
            attempt = pmnc.state.get(retry_id, 1)
            pmnc.state.set(retry_id, attempt + 1)
            if attempt == 1 and kwargs["fail"]:
                1 / 0
            else:
                loopback_queue.push(retry_id)

        with active_interface("retry_order", **interface_config(process_request = process_request,
                              keep_order = True, retry_interval = 3.0, maintenance_period = 1.0)):

            fake_request(15.0)

            xa = pmnc.transaction.create()
            xa.pmnc(queue = "retry_order").foo.m1(fail = True)
            retry_1 = xa.execute()[0]

            xa = pmnc.transaction.create()
            xa.pmnc(queue = "retry_order").foo.m2(fail = False)
            retry_2 = xa.execute()[0]

            assert loopback_queue.pop(5.0) == retry_1
            assert loopback_queue.pop(1.0) == retry_2

    test_order()

    ###################################

    def test_filter_duplicate_id():

        loopback_queue = InterlockedQueue()

        def process_request(module, method, args, kwargs):
            loopback_queue.push(pmnc.request.parameters["retry"]["id"])

        with active_interface("retry_filter_duplicate_id", **interface_config(process_request = process_request,
                              retention_time = 4.0, maintenance_period = 1.0)):

            fake_request(10.0)

            xa = pmnc.transaction.create()
            xa.pmnc(queue = "retry_filter_duplicate_id", id = "foo").foo.bar()
            retry_id1 = xa.execute()[0]

            assert loopback_queue.pop(1.0) == retry_id1
            sleep(3.0)

            xa = pmnc.transaction.create()
            xa.pmnc(queue = "retry_filter_duplicate_id", id = "foo").foo.bar()
            retry_id2 = xa.execute()[0]

            assert loopback_queue.pop(3.0) is None # in the meantime the filter record is removed

            xa = pmnc.transaction.create()
            xa.pmnc(queue = "retry_filter_duplicate_id", id = "foo").foo.bar()
            retry_id3 = xa.execute()[0]

            assert loopback_queue.pop(3.0) == retry_id3

            assert retry_id1 != retry_id2 and retry_id1 != retry_id3 and retry_id2 != retry_id3

    test_filter_duplicate_id()

    ###################################

    def test_expiration():

        loopback_queue = InterlockedQueue()

        def process_request(module, method, args, kwargs):
            loopback_queue.push(pmnc.request.parameters["retry"]["id"])
            1 / 0

        with active_interface("retry_expiration", **interface_config(process_request = process_request,
                              retry_interval = 3.0, maintenance_period = 1.0)):

            fake_request(10.0)

            xa = pmnc.transaction.create()
            xa.pmnc(queue = "retry_expiration", expires_in = 3.0).foo.bar()
            retry_id = xa.execute()[0]

            assert loopback_queue.pop(3.0) == retry_id # the call is expired after one attempt
            assert loopback_queue.pop(6.0) is None

    test_expiration()

    ###################################

    def test_large_message():

        loopback_queue = InterlockedQueue()

        def process_request(module, method, args, kwargs):
            loopback_queue.push(args)

        with active_interface("retry_large_message", **interface_config(process_request = process_request)):

            fake_request(10.0)

            data = b"\x00" * 262144

            xa = pmnc.transaction.create()
            xa.pmnc(queue = "retry_large_message").foo.bar(data)
            retry_id = xa.execute()[0]

            assert loopback_queue.pop(3.0) == (data, )

    test_large_message()

    ###################################

    def test_secondary_call():

        loopback_queue = InterlockedQueue()

        def process_request(module, method, args, kwargs):

            if method == "m":

                xa = pmnc.transaction.create()
                xa.pmnc(queue = "retry_secondary_call").foo.m1() # this will get processed again upon next attempt
                xa.execute()[0]

                xa = pmnc.transaction.create()
                xa.pmnc(queue = "retry_secondary_call", id = "some-id").foo.m2() # this will not get processed again upon next attempt
                xa.execute()[0]

                if pmnc.request.parameters["retry"]["attempt"] == 1:
                    1 / 0

            else:

                loopback_queue.push(method)

        with active_interface("retry_secondary_call", **interface_config(process_request = process_request,
                              retry_interval = 3.0, maintenance_period = 1.0)):

            fake_request(15.0)

            xa = pmnc.transaction.create()
            xa.pmnc(queue = "retry_secondary_call").foo.m()
            retry_id = xa.execute()[0]

            assert loopback_queue.pop(1.0) == "m1"
            assert loopback_queue.pop(1.0) == "m2"
            assert loopback_queue.pop(2.0) is None
            assert loopback_queue.pop(3.0) == "m1"
            assert loopback_queue.pop(5.0) is None

    test_secondary_call()

    ###################################

    def test_commit_failure():

        loopback_queue = InterlockedQueue()

        def process_request(module, method, args, kwargs):
            loopback_queue.push("execute")

        with active_interface("retry_commit_failure", **interface_config(process_request = process_request,
                              retry_interval = 3.0, maintenance_period = 1.0)) as ifc:

            original_handle_success = ifc._handle_success

            fail_commit = True
            def handle_success(commit, rollback, txn, queue_idx, message):
                nonlocal fail_commit
                if fail_commit:
                    rollback(txn) # not quite a commit failure, but to similar effect
                    fail_commit = False
                    loopback_queue.push("commit failure")
                    return True
                else:
                    original_handle_success(commit, rollback, txn, queue_idx, message)
                    loopback_queue.push("commit success")

            ifc._handle_success = handle_success

            fake_request(10.0)

            xa = pmnc.transaction.create()
            xa.pmnc(queue = "retry_commit_failure").foo.bar()
            xa.execute()

            assert loopback_queue.pop(1.0) == "execute"
            assert loopback_queue.pop(1.0) == "commit failure"
            assert loopback_queue.pop(1.0) == "commit success"

    test_commit_failure()

    ###################################

    def test_rollback_failure():

        loopback_queue = InterlockedQueue()

        def process_request(module, method, args, kwargs):
            if pmnc.request.parameters["retry"]["attempt"] == 1:
                loopback_queue.push("execute failure")
                1 / 0
            else:
                loopback_queue.push("execute success")

        with active_interface("retry_rollback_failure", **interface_config(process_request = process_request,
                              retry_interval = 3.0, maintenance_period = 1.0)) as ifc:

            original_handle_failure = ifc._handle_failure

            fail_rollback = True
            def handle_failure(commit, rollback, txn, queue_idx, message):
                nonlocal fail_rollback
                if fail_rollback:
                    rollback(txn) # not quite a rollback failure, but to similar effect
                    fail_rollback = False
                    loopback_queue.push("rollback failure")
                    return True
                else:
                    original_handle_failure(commit, rollback, txn, queue_idx, message)
                    loopback_queue.push("rollback success")

            ifc._handle_failure = handle_failure

            fake_request(15.0)

            xa = pmnc.transaction.create()
            xa.pmnc(queue = "retry_rollback_failure").foo.bar()
            xa.execute()

            assert loopback_queue.pop(1.0) == "execute failure"
            assert loopback_queue.pop(1.0) == "rollback failure"
            assert loopback_queue.pop(1.0) == "rollback success"
            assert loopback_queue.pop(2.0) is None
            assert loopback_queue.pop(3.0) == "execute success"
            assert loopback_queue.pop(3.0) is None

    test_rollback_failure()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
