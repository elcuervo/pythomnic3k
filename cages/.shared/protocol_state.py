#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module implements the transactional resource for module state access,
# which makes it possible to use module state in transactions. For example,
# while a module could access its state directly using
#
# pmnc.state.set("key", value)
#
# the same can be done in transaction, alongside other resources:
#
# xa = pmnc.transaction.create()
# xa.other_resource.do_something(...)
# xa.state.set("key", value)
# xa.execute()
#
# No configuration is required for this resource.
#
# Sample resource usage (anywhere):
#
# xa = pmnc.transaction.create()
# xa.state.get("key"[, default])
# xa.state.set("key", value)
# xa.state.delete("key")
# result = xa.execute()
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Resource" ]

###############################################################################

import pickle; from pickle import dumps as pickle, loads as unpickle

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import typecheck; from typecheck import typecheck_with_exceptions
import pmnc.resource_pool; from pmnc.resource_pool import TransactionalResource, \
                                ResourceError, ResourceInputParameterError

###############################################################################
# the trick here is that this resource uses not its own state, but some other module's
# (the one's that has created the transaction) and the name of transaction-originating
# module is passed along with each call to pmnc.state

class Resource(TransactionalResource): # resource for using module state in transactions

    def begin_transaction(self, *args, **kwargs):
        TransactionalResource.begin_transaction(self, *args, **kwargs)
        self._db = pmnc.state.default_state_db(self.source_module_name)
        self._txn, self._commit, self._rollback = \
            pmnc.state.start_transaction(self.source_module_name)

    @typecheck_with_exceptions(input_parameter_error = ResourceInputParameterError)
    def get(self, key: str, default = None):
        try:
            self._txn, result = \
                pmnc.state.explicit_transaction(self.source_module_name,
                                                self._txn, self._get, key, default)
        except:
            ResourceError.rethrow(recoverable = True, terminal = False)
        else:
            return result

    def _get(self, txn, key, default):
        value = self._db.get(key.encode("unicode-escape"), None, txn)
        if value is None:
            return default
        else:
            return unpickle(value)

    @typecheck_with_exceptions(input_parameter_error = ResourceInputParameterError)
    def set(self, key: str, value):
        try:
            self._txn = \
                pmnc.state.explicit_transaction(self.source_module_name,
                                                self._txn, self._set, key, value)[0]
        except:
            ResourceError.rethrow(recoverable = True, terminal = False)

    def _set(self, txn, key, value):
        self._db.put(key.encode("unicode-escape"), pickle(value), txn)

    @typecheck_with_exceptions(input_parameter_error = ResourceInputParameterError)
    def delete(self, key: str):
        try:
            self._txn = \
                pmnc.state.explicit_transaction(self.source_module_name,
                                                self._txn, self._delete, key)[0]
        except:
            ResourceError.rethrow(recoverable = True, terminal = False)

    def _delete(self, txn, key):
        self._db.delete(key.encode("unicode-escape"), txn)

    def commit(self):
        self._commit(self._txn)

    def rollback(self):
        self._rollback(self._txn)

###############################################################################

def self_test():

    from pmnc.request import fake_request
    from expected import expected
    from pmnc.resource_pool import TransactionExecutionError

    ###################################

    rus = "\u0410\u0411\u0412\u0413\u0414\u0415\u0401\u0416\u0417\u0418\u0419" \
          "\u041a\u041b\u041c\u041d\u041e\u041f\u0420\u0421\u0422\u0423\u0424" \
          "\u0425\u0426\u0427\u0428\u0429\u042c\u042b\u042a\u042d\u042e\u042f" \
          "\u0430\u0431\u0432\u0433\u0434\u0435\u0451\u0436\u0437\u0438\u0439" \
          "\u043a\u043b\u043c\u043d\u043e\u043f\u0440\u0441\u0442\u0443\u0444" \
          "\u0445\u0446\u0447\u0448\u0449\u044c\u044b\u044a\u044d\u044e\u044f"

    rus_b = rus.encode("windows-1251")

    ###################################

    def test_get_set_delete():

        fake_request(10.0)

        xa = pmnc.transaction.create()
        xa.state.get("key")
        assert xa.execute() == (None, )

        xa = pmnc.transaction.create()
        xa.state.get("key", "default")
        assert xa.execute() == ("default", )

        xa = pmnc.transaction.create()
        xa.state.set("key", "value")
        assert xa.execute() == (None, )

        xa = pmnc.transaction.create()
        xa.state.get("key")
        assert xa.execute() == ("value", )

        xa = pmnc.transaction.create()
        xa.state.get("key", "default")
        assert xa.execute() == ("value", )

        xa = pmnc.transaction.create()
        xa.state.delete("key")
        assert xa.execute() == (None, )

        xa = pmnc.transaction.create()
        xa.state.get("key")
        assert xa.execute() == (None, )

        xa = pmnc.transaction.create()
        xa.state.get("key", "default")
        assert xa.execute() == ("default", )

    test_get_set_delete()

    ###################################

    def test_same_state():

        pmnc.state.set("foo", "bar")

        xa = pmnc.transaction.create()
        xa.state.get("foo")
        assert xa.execute() == ("bar", )

        xa = pmnc.transaction.create()
        xa.state.set("foo", "BAR")
        assert xa.execute() == (None, )

        assert pmnc.state.get("foo") == "BAR"

        xa = pmnc.transaction.create()
        xa.state.delete("foo")
        assert xa.execute() == (None, )

        assert pmnc.state.get("foo") is None

    test_same_state()

    ###################################

    def test_get_set_delete_deadlock():

        fake_request(5.0)

        xa = pmnc.transaction.create()
        xa.state.set("key", "value1")
        xa.state.set("key", "value2")
        try:
            xa.execute()
        except (ResourceError, TransactionExecutionError): # depends on who times out first
            pass
        else:
            assert False

        fake_request(5.0)

        xa = pmnc.transaction.create()
        xa.state.set("key", "value")
        xa.execute()

        xa = pmnc.transaction.create()
        xa.state.delete("key")
        xa.state.get("key")
        try:
            xa.execute()
        except (ResourceError, TransactionExecutionError): # depends on who times out first
            pass
        else:
            assert False

        fake_request(5.0)

        xa = pmnc.transaction.create()
        xa.state.get("key")
        xa.state.get("key")
        assert xa.execute() == ("value", "value")

        xa = pmnc.transaction.create()
        xa.state.get("key")
        xa.state.set("key", "value2")
        try:
            xa.execute()
        except (ResourceError, TransactionExecutionError): # depends on who times out first
            pass
        else:
            assert False

    test_get_set_delete_deadlock()

    ###################################

    def test_bytes_str():

        fake_request(5.0)

        xa = pmnc.transaction.create()
        xa.state.set(rus, rus + rus)
        assert xa.execute() == (None, )

        xa = pmnc.transaction.create()
        xa.state.get(rus)
        assert xa.execute() == (rus + rus, )

        xa = pmnc.transaction.create()
        xa.state.delete(rus)
        assert xa.execute() == (None, )

        xa = pmnc.transaction.create()
        xa.state.get(rus_b)
        with expected(ResourceInputParameterError):
            xa.execute()

        xa = pmnc.transaction.create()
        xa.state.set(rus_b, "value")
        with expected(ResourceInputParameterError):
            xa.execute()

        xa = pmnc.transaction.create()
        xa.state.delete(rus_b)
        with expected(ResourceInputParameterError):
            xa.execute()

    test_bytes_str()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF