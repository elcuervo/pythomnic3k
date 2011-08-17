# configuration file for standard HTTP interface "performance"
#
# this file configures a http interface for serving dynamic
# web pages with cage performance statistics collected in module
# performance.py
#
# each cage needs its own copy of this file, each with its own
# different port number
#
# DejaVu appears to be the only mainstream monospace font
# to support Unicode block characters (0x258x), but if you
# have a better one, specify it in css_font_family

config = dict \
(
protocol = "http",                                # meta
listener_address = ("0.0.0.0", 0),                # tcp (note that each cage needs a separate port)
max_connections = 100,                            # tcp
ssl_key_cert_file = None,                         # ssl, optional filename
ssl_ca_cert_file = None,                          # ssl, optional filename
response_encoding = "ascii",                      # http
original_ip_header_fields = (),                   # http
allowed_methods = ("GET", ),                      # http
keep_alive_support = True,                        # http
keep_alive_idle_timeout = 120.0,                  # http
keep_alive_max_requests = 10,                     # http
refresh_seconds = 10,                             # performance
css_font_family = "DejaVu Sans Mono, monospace",  # performance
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, {}, key, default)
copy = lambda: pmnc.config.copy_(config, {})

# EOF
