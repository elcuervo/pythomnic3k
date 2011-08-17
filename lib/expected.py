#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# Utility functions for other modules' self-testing.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "expected" ]

###############################################################################

import re; from re import compile as regex

###############################################################################

class expected:

    def __init__(self, e, v = None):
        if isinstance(e, Exception) and v is None:
            self._t = e.__class__
            self._match = lambda s: s.startswith(str(e))
        elif isinstance(e, type) and issubclass(e, Exception) and \
             ((v is None) or isinstance(v, str)):
            self._t = e
            if v is None:
                self._match = lambda s: True
            else:
                v = regex("^(?:{0:s})$".format(v))
                self._match = lambda s: v.match(s) is not None
        else:
            raise Exception("usage: with expected(Exception[, \"regex\"]): "
                            "or with expected(Exception(\"text\")):")

    def __enter__(self):
        try:
            pass
        except:
            pass # this is a Python 3 way of saying sys.exc_clear()

    def __exit__(self, t, v, tb):
        assert t is not None, "expected {0:s} to have been thrown".format(self._t.__name__)
        return issubclass(t, self._t) and self._match(str(v))

###############################################################################

if __name__ == "__main__":

    print("self-testing module expected.py:")

    try:
        with expected("foo"):
            pass
    except Exception as e:
        assert str(e) == "usage: with expected(Exception[, \"regex\"]): " \
                         "or with expected(Exception(\"text\")):"
    else:
        assert False

    with expected(IndexError):
        ()[0]

    with expected(IndexError("tuple index out of range")):
        ()[0]

    with expected(IndexError, "tuple.*range"):
        ()[0]

    with expected(IndexError("tuple index")):
        ()[0]

    try:
        with expected(IndexError):
            (1, )[0]
    except AssertionError as e:
        assert str(e) == "expected IndexError to have been thrown"
    else:
        assert False

    try:
        with expected(IndexError("range out of tuple index")):
            ()[0]
    except IndexError as e:
        assert str(e) == "tuple index out of range"
    else:
        assert False

    try:
        with expected(IndexError, "tuple range"):
            ()[0]
    except IndexError as e:
        assert str(e) == "tuple index out of range"
    else:
        assert False

    try:
        with expected(IndexError):
            {}["foo"]
    except KeyError as e:
        assert str(e) == "'foo'"
    else:
        assert False

    with expected(IndexError):
        with expected(KeyError):
            with expected(RuntimeError):
                ()[0]

    with expected(Exception):
        ()[0]

    with expected(Exception("tuple index out of range")):
        ()[0]

    with expected(Exception, "tuple [Ii][Nn][Dd][Ee][Xx].*"):
        ()[0]

    print("ok")

###############################################################################
# EOF
