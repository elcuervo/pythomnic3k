# this file contains various configuration parameters for the module
# state.py, there is hardly any reason for changing anything here

config = dict \
(
checkpoint_interval = 30.0, # seconds between state checkpoints
minimum_free_space = 50,    # megabytes of free disk space for state to work at all
)

# self-tests of state.py depend on the following configuration,
# this dict may safely be removed in production

self_test_config = dict \
(
checkpoint_interval = 3.0,
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF