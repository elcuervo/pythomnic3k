# configuration file for resource "pmnc", which is responsible
# for initiating retriable calls
#
# there is really just one resource of protocol "pmnc" per cage,
# no need to ever configure another or to change this one, therefore
# this file is pretty much static and need not to be copied to
# each cage
#
# note that the resource is called pmnc, but the implementation is
# delegated to protocol "retry", the reason for this is that pmnc
# looks better in
#
# xa = pmnc.transaction.create()
# xa.pmnc("somecage", queue = "retry").module.method()
# retry_id = xa.execute()[0]
#
# for directly resembling regular use of
# pmnc("somecage", queue = "retry").module.method()

config = dict \
(
protocol = "retry", # meta, nothing to configure here
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, {}, key, default)
copy = lambda: pmnc.config.copy_(config, {})

# EOF