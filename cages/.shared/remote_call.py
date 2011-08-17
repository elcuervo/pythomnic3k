#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module is invoked by the module loader when it is processing RPC syntax
#
# result = pmnc("target_cage").module.method(*args, **kwargs)
#                                                           ^ remote_call.py is called here
#
# If a name of a queue is specified, the call is serialized and enqueued
# through a persistent queue with that name. The call returns immediately,
# returning as a result some unique identifier.
#
# retry_id = pmnc("target_cage", queue = "queue_name").module.method(*args, **kwargs)
#                                ^^^^^^^^^^^^^^^^^^^^
# Additional options control the execution of such persistent calls:
#
# pmnc("target_cage", queue = "queue_name", id = "FOO", expires_in = 600.0)
#                                           ^^^^^^^^^^  ^^^^^^^^^^^^^^^^^^
# 1. id contains some caller-specific unique id to prevent duplicate executions:
#
# retry_id1 = pmnc(..., id = "FOO")
# retry_id2 = pmnc(..., id = "FOO")
#
# now retry_id1 and retry_id2 are different but the second call is not enqueued
# at all, this is useful if you account for a repeated execution of the same code,
# which is only natural with the retry mechanism.
#
# 2. expires_in contains expiration time for the call in seconds (by default
# the attempts to execute the call are repeated infinitely).
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "execute_sync", "execute_async", "accept", "transmit" ]

###############################################################################

import time; from time import time

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import typecheck; from typecheck import optional, by_regex

###############################################################################

valid_cage_name = by_regex("^[A-Za-z0-9_-]{1,32}$")
valid_module_name = by_regex("^[A-Za-z0-9_]{1,64}$")
valid_method_name = by_regex("^[A-Za-z0-9_]{1,64}$")
valid_queue_name = by_regex("^[A-Za-z0-9_-]{1,64}$")
valid_retry_id = by_regex("^RT-[0-9]{14}-[0-9A-F]{12}$")

###############################################################################
# this method is called by the module loader to handle synchronous RPC call
# pmnc("target_cage").module.method(*args, **kwargs)

def execute_sync(target_cage: valid_cage_name, module: valid_module_name,
                 method: valid_method_name, args: tuple, kwargs: dict):

    # execution of synchronous RPC calls to a cage foo is done through
    # a separate thread pool rpc__foo, as though each target cage was
    # a separate resource; this is beneficial because it gives an
    # effective mechanism of protecting both this cage from hanging
    # and the other cage from becoming overwhelmed

    xa = pmnc.transaction.create()
    xa.__getattr__("rpc__{0:s}".format(target_cage))().\
       __getattr__(module).__getattr__(method)(*args, **kwargs)
    return xa.execute()[0]

###############################################################################
# this method is called by the module loader to handle asynchronous RPC call
# pmnc("target_cage", queue = "queue_name").module.method(*args, **kwargs)

def execute_async(target_cage: valid_cage_name, module: valid_module_name,
                  method: valid_method_name, args: tuple, kwargs: dict, *,
                  queue: optional(valid_queue_name) = None, id: optional(str) = None,
                  expires_in: optional(float) = None) -> valid_retry_id:

    # if the caller used deprecated syntax pmnc("target_cage:retry"),
    # the name of the queue is set to None meaning "default queue"

    if queue is None: # in which case the configured default value is used
        queue = pmnc.config.get("retry_queue")

    # execution of asynchronous calls is done through
    # a special resource "pmnc" of protocol "retry"

    xa = pmnc.transaction.create()
    xa.pmnc(target_cage, queue = queue, id = id, expires_in = expires_in).\
       __getattr__(module).__getattr__(method)(*args, **kwargs)
    return xa.execute()[0]

###############################################################################
# this method is called from protocol_retry to deliver a retried call to another cage,
# it is itself executed in a retried call and is therefore retried again and again

def transmit(target_cage: str, module: str, method: str, args: tuple, kwargs: dict):

    # the actual transmission is still done through synchronous RPC

    retry_ctx = pmnc.request.parameters["retry"]
    source_cage_id = retry_ctx["id"]
    target_cage_id = pmnc(target_cage).remote_call.accept(module, method, args, kwargs,
                                                          source_cage_id = source_cage_id,
                                                          retry_deadline = retry_ctx["deadline"])

    pmnc.log.debug("enqueued RPC call {0:s}.{1:s} to {2:s} as RT-{3:s} "
                   "has been accepted as RT-{4:s}".format(module, method,
                   target_cage, source_cage_id[-4:], target_cage_id[-4:]))

###############################################################################
# this method accepts incoming retried calls to this cage from other cages
# initiated in transmit above

def accept(module: str, method: str, args: tuple, kwargs: dict, *,
           source_cage_id: valid_retry_id, retry_deadline: optional(int),
           **options):

    # note that the original unique id of the retried call is used
    # to prevent duplicate invocations and the deadline is also inherited

    if retry_deadline is not None:
        expires_in = max(retry_deadline - time(), 0.0)
    else:
        expires_in = None

    # the actual attempts to execute the local method are deferred, the number
    # of attempts, retry interval and other policy parameters are now under
    # local cage's control, for example the current attempt is again 1

    return pmnc(queue = pmnc.config.get("retry_queue"),
                id = source_cage_id, expires_in = expires_in).\
                __getattr__(module).__getattr__(method)(*args, **kwargs)

###############################################################################
# EOF
