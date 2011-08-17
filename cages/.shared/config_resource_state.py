# configuration file for resource "state"
#
# there is really just one resource of protocol "state" per cage,
# no need to ever configure another or to change this one, therefore
# this file is pretty much static and need not to be copied to
# each cage

config = dict \
(
protocol = "state", # meta, nothing to configure here
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, {}, key, default)
copy = lambda: pmnc.config.copy_(config, {})

# EOF