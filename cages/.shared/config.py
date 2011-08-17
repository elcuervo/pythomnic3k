#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module redirects access to pmnc.config from some module to that module's
# private configuration module, so that any module can access its own private
# configuration by simply pmnc.config.
#
# For example,
#
# pmnc.config.get("foo") # in module bar.py
#
# actually fetches parameter "foo" from configuration file config_bar.py
#
# Note that the actual configuration files such as config_bar.py use this
# module's methods get_ and copy_ for extracting values. This comes in
# useful when you want to introduce your own custom source of configuration,
# in which case you modify just one this module to intercept the calls.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "get", "copy", "get_", "copy_" ]

###############################################################################
# the following methods redirect access to particular configuration files

def get(key: str, default = None, *, __source_module_name):

    config_module_name = "config_{0:s}".format(__source_module_name)
    return pmnc.__getattr__(config_module_name).get(key, default)

def copy(*, __source_module_name):

    config_module_name = "config_{0:s}".format(__source_module_name)
    return pmnc.__getattr__(config_module_name).copy()

###############################################################################
# the following methods are called back from redirected calls
# with their private configuration dicts as parameters

def get_(config, self_test_config, key, default):

    if pmnc.request.self_test and key in self_test_config:
        return self_test_config[key]
    else:
        return config.get(key, default)

def copy_(config, self_test_config):

    result = config.copy()
    if pmnc.request.self_test:
        result.update(self_test_config)
    return result

###############################################################################
# EOF