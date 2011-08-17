# this configuration module exists for self-testing, but you can use it as
# a reference for configuring resources of "callable" protocol, in which case
# you copy this file and edit the copy

config = dict \
(
protocol = "callable",              # meta
connect = lambda resource: None,    # callable, this gets executed for connect()
disconnect = lambda resource: None, # callable, this gets executed for disconnect()
)

# self-tests of transaction.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

import time; from time import sleep

self_test_config = dict \
(
connect = lambda resource: sleep(1.5), # callable, hanging connect
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF