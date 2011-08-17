#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# Mixin class for defining comparable classes.
#
# Pythomnic3k project
# (c) 2005-2009, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "ComparableMixin" ]

################################################################################

class ComparableMixin:

    def __lt__(self, other):
        pass
    def __eq__(self, other):
        pass

    def __gt__(self, other):
        return not self.__lt__(other) and not self.__eq__(other)
    def __ne__(self, other):
        return not self.__eq__(other)
    def __le__(self, other):
        return self.__lt__(other) or self.__eq__(other)
    def __ge__(self, other):
        return self.__gt__(other) or self.__eq__(other)

################################################################################

if __name__ == "__main__":

    print("self-testing module comparable_mixin.py:")

    class Value(ComparableMixin):
        def __init__(self, value):
            self._value = value
        def __lt__(self, other):
            return self._value < other._value
        def __eq__(self, other):
            return self._value == other._value

    assert Value(1) == Value(1)
    assert Value(1) != Value(2)
    assert Value(2) != Value(1)

    assert Value(1) < Value(2)
    assert Value(1) <= Value(2)
    assert Value(1) <= Value(1)

    assert Value(2) > Value(1)
    assert Value(2) >= Value(1)
    assert Value(1) >= Value(1)

    print("ok")

################################################################################
# EOF
