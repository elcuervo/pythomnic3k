# This module contains configuration for the notify.py module.
#
# By configuring a separate retry interface and changing retry_queue below
# you can use a different queue for sending messages to the health_monitor,
# although it is very unlikely that you ever want to do this.
#
# By increasing messages_to_keep you can have a longer history of messages
# on the performance web log page.

config = dict \
(
retry_queue = "retry",   # name of a retried queue to use for sending messages to the health monitor
messages_to_keep = 100,  # number of recent messages to keep in memory and display on the web page
)

# self-tests of notify.py depend on the following configuration,
# this dict may safely be removed in production

self_test_config = dict \
(
messages_to_keep = 4,
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF