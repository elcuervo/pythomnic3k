# this configuration module exists for self-testing, but you can use it as
# a reference for configuring resources of "callable" protocol, in which case
# you copy this file and edit the copy, but see config_resource_callable_2
# and config_resource_callable_3 for simpler alternatives

config = dict \
(
protocol = "callable",              # meta
connect = lambda resource: None,    # callable, this gets executed for connect()
disconnect = lambda resource: None, # callable, this gets executed for disconnect()
)

# self-tests of protocol_callable.py depend on the following configuration

# this is a particularly complex configuration, precisely because of the nature
# of this "protocol" whereby all actual work is done by external callable hooks
# and the necessity to access a particular resource instance for testing

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import interlocked_queue; from interlocked_queue import InterlockedQueue

def connect(resource):
    resource._q = resource._config.pop("trace_queue") # _config contains config dict
    resource._count = 0
    resource._q.push(("connect", resource._count, resource._config))

def disconnect(resource):
    resource._count += 1
    resource._q.push(("disconnect", resource._count))

self_test_config = dict \
(
param1 = "value1",
param2 = "value2",
connect = connect,
disconnect = disconnect,
trace_queue = InterlockedQueue() # self-test reads this queue to see what's going on
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF