#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module "implements" a void resource used only for self-testing.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Resource" ]

###############################################################################

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import pmnc.resource_pool; from pmnc.resource_pool import TransactionalResource

###############################################################################

class Resource(TransactionalResource): # stuff-doing resource

    def do_stuff(self, *args, **kwargs):
        pass

###############################################################################

def self_test():

    from pmnc.request import fake_request

    fake_request(10.0)

    xa = pmnc.transaction.create()
    xa.void.do_stuff()
    assert xa.execute() == (None, )

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF