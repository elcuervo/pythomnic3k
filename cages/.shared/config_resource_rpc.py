# configuration file for resource "rpc"
#
# all the cages that wish to share the same RPC "namespace" need
# to have identical broadcast ports, broadcast addresses that
# face the same subnet, and the same flock id
#
# flock id is an arbitrary identifier around which all the related
# cages are grouped, same port broadcasts with different flock id
# will be ignored
#
# there is no need to make a copy of this file for each cage,
# but you may need to modify the broadcast_address parameter
# if your OS doesn't work with 255.255.255.255 broadcasts,
# for example, under FreeBSD change it to something like
# "192.168.0.1/192.168.255.255"

config = dict \
(
protocol = "rpc",                                       # meta
broadcast_address = ("0.0.0.0/255.255.255.255", 12480), # rpc, "interface address/broadcast address", port
discovery_timeout = 3.0,                                # rpc + tcp (connect timeout)
multiple_timeout_allowance = 0.5,                       # rpc, in range 0.0..1.0
flock_id = "DEFAULT",                                   # rpc
exact_locations = {},                                   # rpc, maps cage names to their fixed locations
pool__idle_timeout = 120.0,                             # meta, increased default idle timeout
pool__max_age = 600.0,                                  # meta, increased default max age
)

# self-tests of protocol_rpc.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
broadcast_address = ("0.0.0.0/255.255.255.255", 12481),
flock_id = "SELF_TEST",
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF