# This module contains configuration for a  resource, and is only
# used for self-testing of modules transaction.py and shared_pools.py,
# it also illustrates the resource pool meta settings which are
# applicable to all resources, no matter which protocol

config = dict \
(
protocol = "void",        # meta
)

# self-tests of transaction.py and shared_pools.py depend on the following configuration

self_test_config = dict \
(
pool__size = 3,           # meta, optional, restricts the maximum pool size
pool__idle_timeout = 1.0, # meta, optional, restricts the resource idle timeout
pool__max_age = 2.0,      # meta, optional, restricts the resource max age
pool__min_time = 0.5,     # meta, optional, restricts the minimum remaining request time to access the resource
pool__standby = 1,        # meta, optional, indicates the number of resources to be kept connected
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF