# this module configures health monitor module, when started
# on a cage called health_monitor, it sends probes to each
# cage approximately once in probe_period

config = dict \
(
probe_period = 30.0,    # seconds between probing passes
connect_timeout = 3.0,  # connect timeout for probe request
)

# self-tests of health_monitor.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
probe_period = 1.0,
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF