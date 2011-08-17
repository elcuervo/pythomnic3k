# configuration file for retry interface for a queue named "retry"
#
# this retry interface is used for retrying calls enqueued using
# pmnc("somecage", queue = "retry") and xa.pmnc("somecage", queue = "retry")
#
# this queue called "retry" is kind of a "default" queue because it is
# configured to be used for retrying RPC calls accepted from other cages
# and to deliver notifications to the health monitor, but you can configure
# more queues - interfaces of protocol "retry" with different parameters
# such as keep_order or retry_interval by making copies of this file,
# exactly the same as with all the other interfaces
#
# don't decrease queue_count unless you are sure the persistent queues
# are empty, otherwise the calls queued in the excluded queues will not
# be processed

config = dict \
(
protocol = "retry",        # meta
queue_count = 4,           # maximum number of retried calls scheduled concurrently
keep_order = False,        # True if execution order is to be preserved (implies queue_count = 1)
retry_interval = 120.0,    # seconds between attempts to execute retried calls
retention_time = 259200.0, # seconds to keep history of successfully executed calls
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, {}, key, default)
copy = lambda: pmnc.config.copy_(config, {})

# EOF
