#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module redirects a request to create and run a named interface
# to a protocol-specific module, in a way similar to resource.py.
#
# Pythomnic3k project
# (c) 2005-2009, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "create" ]

###############################################################################

def create(interface_name: str, *, protocol: str, **config):

    protocol_module_name = "protocol_{0:s}".format(protocol)
    return pmnc.__getattr__(protocol_module_name).Interface(interface_name, **config)

###############################################################################
# EOF