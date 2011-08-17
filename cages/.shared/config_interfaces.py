# this is the default configuration file for cage interfaces
#
# request_timeout is conceptually the most far reaching parameter
# here, it is one of the guarantees for the entire cage - that it
# responds within that time, even though the response is a failure
#
# thread_count limits the number of threads in the cage's main
# processing thread pool, thus effectively the number of requests
# being processed concurrently, no matter which interface they
# arrived from; this behaviour of processing stuff with pools of
# worker threads while putting the excessive work on queue is one
# of the design principles of Pythomnic3k
#
# log_level can be changed at runtime to temporarily increase logging
# verbosity (set to "DEBUG") to see wtf is going on

config = dict \
(
interfaces = ("performance", "rpc", "retry"), # tuple containing names of interfaces to start
request_timeout = 10.0,                       # global request timeout for this cage
thread_count = 10,                            # interfaces worker thread pool size
sweep_period = 15.0,                          # time between scanning all pools for expired objects
log_level = "INFO",                           # one of "ERROR", "WARNING", "LOG", "INFO", "DEBUG"
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, {}, key, default)
copy = lambda: pmnc.config.copy_(config, {})

# EOF