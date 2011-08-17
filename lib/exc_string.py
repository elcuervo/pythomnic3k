#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# Utility function to format exception stack dump into a single string.
#
# Pythomnic3k project
# (c) 2005-2009, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "exc_string", "trace_string" ]

###############################################################################

import sys
import traceback
import os

###############################################################################

def trace_string(*, tb = None, skip = 0):
    return " <- ".join("{0}() ({1}:{2})".format(m, os.path.split(f)[1], n)
                       for f, n, m, u in reversed(tb or traceback.extract_stack()[:-1-skip]))

###############################################################################

def exc_string():

    t, v, tb = sys.exc_info()

    if t is None:
        return ""

    try:
        v = str(v)
    except:
        v = ""

    return "{0.__name__:s}(\"{1:s}\") in {2:s}".format(t, v, trace_string(tb = traceback.extract_tb(tb)))

################################################################################

if __name__ == "__main__":

    print("self-testing module exc_string.py:")

    ###################################

    assert exc_string() == ""

    ###################################

    try:
        ()[0]
    except:
        assert exc_string() == "IndexError(\"tuple index out of range\") in <module>() (exc_string.py:56)"

    ###################################

    russian = "\u0410\u0411\u0412\u0413\u0414\u0415\u0401\u0416\u0417\u0418\u0419\u041a" \
              "\u041b\u041c\u041d\u041e\u041f\u0420\u0421\u0422\u0423\u0424\u0425\u0426" \
              "\u0427\u0428\u0429\u042c\u042b\u042a\u042d\u042e\u042f\u0430\u0431\u0432" \
              "\u0433\u0434\u0435\u0451\u0436\u0437\u0438\u0439\u043a\u043b\u043c\u043d" \
              "\u043e\u043f\u0440\u0441\u0442\u0443\u0444\u0445\u0446\u0447\u0448\u0449" \
              "\u044c\u044b\u044a\u044d\u044e\u044f"

    class Foo:
        @classmethod
        def foo(self):
            raise Exception(russian)

    try:
        Foo.foo()
    except:
        assert exc_string() == "Exception(\"{0}\") in foo() (exc_string.py:72) " \
                               "<- <module>() (exc_string.py:75)".format(russian)
    else:
        assert False

    ###################################

    class FooException(Exception):
        def __str__(self):
            raise Exception("str fails")

    def bar():
        raise FooException(123)

    def foo():
        bar()

    try:
        foo()
    except:
        assert exc_string() == "FooException(\"\") in bar() (exc_string.py:89) " \
                               "<- foo() (exc_string.py:92) <- <module>() (exc_string.py:95)"
    else:
        assert False

    ###################################

    def foo():
        assert trace_string(skip = 2) == "biz() (exc_string.py:112) <- baz() " \
                                         "(exc_string.py:115) <- <module>() (exc_string.py:117)"

    def bar():
        foo()

    def biz():
        bar()

    def baz():
        biz()

    baz()

    ###################################

    print("ok")

################################################################################
# EOF
