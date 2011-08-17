# This module contains configuration for the remote_call.py module.
#
# By configuring a separate retry interface and changing retry_queue below
# you can use a different queue for retrying accepted incoming RPC calls,
# although it is very unlikely that you ever want to do this.

config = dict \
(
retry_queue = "retry", # name of a retried queue to use for retrying accepted RPC calls
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, {}, key, default)
copy = lambda: pmnc.config.copy_(config, {})

# EOF