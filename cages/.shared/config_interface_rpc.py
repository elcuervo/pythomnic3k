# configuration file for interface "rpc"
#
# all the cages that wish to share the same RPC "namespace" need
# to have identical broadcast ports, broadcast addresses that
# face the same subnet, and the same flock id
#
# flock id is an arbitrary identifier around which all the related
# cages are grouped, same port broadcasts with different flock id
# will be ignored
#
# the SSL listener is bound to a random port in specified range
#
# there is no need to make a copy of this file for each cage,
# but you may need to modify the broadcast_address parameter
# if your OS doesn't work with 255.255.255.255 broadcasts,
# for example, under FreeBSD change it to something like
# "192.168.0.1/192.168.255.255"

config = dict \
(
protocol = "rpc",                                       # meta
random_port = -63000,                                   # tcp, negative means "in range 63000..63999"
max_connections = 100,                                  # tcp
broadcast_address = ("0.0.0.0/255.255.255.255", 12480), # rpc, "interface address/broadcast address", port
flock_id = "DEFAULT",                                   # rpc
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, {}, key, default)
copy = lambda: pmnc.config.copy_(config, {})

# EOF