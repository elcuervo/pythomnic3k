#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module is a dispenser of thread pools and resource pools (which are
# grouped in pairs) and used by the transaction machinery and other modules
# that need them a private thread pool for something.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "get_thread_pool", "get_resource_pool", "get_private_thread_pool" ]
__reloadable__ = False

################################################################################

import threading; from threading import Lock

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import typecheck; from typecheck import typecheck, optional, callable
import pmnc.thread_pool; from pmnc.thread_pool import ThreadPool
import pmnc.resource_pool; from pmnc.resource_pool import RegisteredResourcePool

###############################################################################

# module-level state => not reloadable

_private_pools = {}
_combined_pools = {}
_pools_lock = Lock()

###############################################################################
# this method returns a configuration dict for the specified resource

@typecheck
def _get_resource_config(resource_name: str) -> dict:

    # resources with names like "rpc__cage" share the same base configuration
    # of "config_resource_rpc" and have pool__resource_name=cage parameter added
    #                     ^^^                               ^^^^

    if "__" in resource_name:
        config_name, resource_name = resource_name.split("__", 1)
        config_module_name = "config_resource_{0:s}".format(config_name)
        config = pmnc.__getattr__(config_module_name).copy()
        config["pool__resource_name"] = resource_name
    else:
        config_module_name = "config_resource_{0:s}".format(resource_name)
        config = pmnc.__getattr__(config_module_name).copy()

    return config

###############################################################################
# this method returns a factory method for creating specified resource instances

@typecheck
def _get_resource_factory(resource_name: str, pool_size: int, pool_standby: int) -> callable:

    def resource_factory(resource_instance_name):

        # fetch the configuration at the time of resource creation

        config = _get_resource_config(resource_name)

        # strip the pool__... meta settings ignoring static settings
        # pool__size and pool__standby which are already in effect

        idle_timeout = config.pop("pool__idle_timeout", None)
        max_age = config.pop("pool__max_age", None)
        min_time = config.pop("pool__min_time", None)

        if config.pop("pool__size", pool_size) != pool_size:
            pmnc.log.warning("change in pool size for resource {0:s} at "
                             "runtime has no effect".format(resource_name))
        if config.pop("pool__standby", pool_standby) != pool_standby:
            pmnc.log.warning("change in pool standby for resource {0:s} at "
                             "runtime has no effect".format(resource_name))

        pmnc.log.debug("creating resource instance {0:s}".format(resource_instance_name))

        # pass the rest of the config to the specific resource-creating factory

        resource_instance = pmnc.resource.create(resource_instance_name, **config)

        # if the meta settings have been present, adjust the created instance

        if idle_timeout is not None: resource_instance.set_idle_timeout(idle_timeout)
        if max_age is not None: resource_instance.set_max_age(max_age)
        if min_time is not None: resource_instance.set_min_time(min_time)
        resource_instance.set_pool_info(resource_name, pool_size)

        pmnc.log.debug("resource instance {0:s} has been created".\
                       format(resource_instance_name))

        return resource_instance

    return resource_factory

###############################################################################
# this method returns (creating if necessary) a pair of a thread pool and
# resource pool for the specified resource

@typecheck
def _get_pools(resource_name: str) -> (ThreadPool, RegisteredResourcePool):

    pool_name = resource_name

    with _pools_lock:

        if pool_name not in _combined_pools:

            config = _get_resource_config(resource_name)

            # pool size for a resource is a static setting and is by default
            # equal to the number of the interfaces worker threads

            pool_size = config.get("pool__size") or \
                        pmnc.config_interfaces.get("thread_count")

            # the number of resources to be kept warm is 0 by default

            pool_standby = config.get("pool__standby") or 0

            # create and cache new thread pool and resource pool for the resource,
            # they are of the same size, so that each thread can always pick a resource

            resource_factory = _get_resource_factory(resource_name, pool_size, pool_standby)

            # note that the hard limit on the resource pool size is greater than the number
            # of worker threads, this way worker threads still have each its guaranteed resource
            # instance whenever necessary, but the sweeper threads also can have their busy slots
            # whenever they intervene for removing the expired connections or warming the pool up

            thread_pool = ThreadPool(resource_name, pool_size)
            resource_pool = RegisteredResourcePool(resource_name, resource_factory,
                                                   pool_size + 2, pool_standby)

            _combined_pools[pool_name] = (thread_pool, resource_pool)

        return _combined_pools[pool_name]

###############################################################################

def get_thread_pool(resource_name: str) -> ThreadPool:
    return _get_pools(resource_name)[0]

###############################################################################

def get_resource_pool(resource_name: str) -> RegisteredResourcePool:
    return _get_pools(resource_name)[1]

###############################################################################

def get_private_thread_pool(pool_name: optional(str) = None,
                            pool_size: optional(int) = None,
                            *, __source_module_name) -> ThreadPool:

    pool_name = "{0:s}{1:s}".format(__source_module_name,
                                    pool_name is not None and "/{0:s}".format(pool_name) or "")
    with _pools_lock:

        if pool_name not in _private_pools:
            pool_size = pool_size or pmnc.config_interfaces.get("thread_count")
            _private_pools[pool_name] = ThreadPool(pool_name, pool_size)

        return _private_pools[pool_name]

###############################################################################

def self_test():

    from pmnc.request import fake_request
    from time import sleep
    from expected import expected
    from pmnc.timeout import Timeout

    ###################################

    # when this timeout elapses, sweeper should have made its pass

    sweep_timeout = Timeout(pmnc.config_interfaces.get("sweep_period") + 5.0)

    ###################################

    def test_pools():

        tp1 = pmnc.shared_pools.get_thread_pool("void")
        tp2 = pmnc.shared_pools.get_thread_pool("void")
        assert tp1 is tp2

        assert tp1.free == 0

        assert list(_combined_pools.keys()) == [ "void" ]
        assert list(_private_pools.keys()) == []

        rp1 = pmnc.shared_pools.get_resource_pool("void")
        rp2 = pmnc.shared_pools.get_resource_pool("void")
        assert rp1 is rp2

        r = rp1.allocate()
        assert r.pool_name == "void" and r.pool_size == 3
        rp1.release(r)

    test_pools()

    ###################################

    def test_private_pool():

        tp1 = pmnc.shared_pools.get_private_thread_pool("foo")
        assert tp1.size == pmnc.config_interfaces.get("thread_count")

        tp2 = pmnc.shared_pools.get_private_thread_pool("foo", 10000)
        assert tp2 is tp1

        assert list(_combined_pools.keys()) == [ "void" ]
        assert list(_private_pools.keys()) == [ "shared_pools/foo" ]

        tp3 = pmnc.shared_pools.get_private_thread_pool(None, 3)
        assert tp3 is not tp2
        assert tp3.size == 3

        assert list(_combined_pools.keys()) == [ "void" ]
        assert list(sorted(_private_pools.keys())) == [ "shared_pools", "shared_pools/foo" ]

        def wu_test():
            return "123"

        wu = tp1.enqueue(fake_request(1.0), wu_test, (), {})
        assert wu.wait() == "123"

    test_private_pool()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
