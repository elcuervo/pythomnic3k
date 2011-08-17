#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
###############################################################################
#
# ModuleLoader is the heart of Pythomnic3k. An instance of ModuleLoader is
# accessible from all modules as pmnc and is essentialy the running cage itself.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "ModuleLoader" ]

###############################################################################

import threading; from threading import Lock, current_thread
import os; from os import path as os_path, stat
import sys; from sys import platform, modules as sys_modules, getrefcount
import imp; from imp import acquire_lock as acquire_imp_lock, load_module, \
                            release_lock as release_imp_lock, PY_SOURCE, PY_COMPILED
import inspect; from inspect import isfunction, getfullargspec, isclass
import traceback; from traceback import extract_stack

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import typecheck, optional, by_regex, callable, list_of
import shared_lock; from shared_lock import SharedLockWriterPriority
import pmnc.module_locator; from pmnc.module_locator import ModuleLocator
import pmnc.timeout; from pmnc.timeout import Timeout

###############################################################################

def fix_broken_imports():
    import socket; from socket import socket, AF_INET, SOCK_DGRAM
    socket(AF_INET, SOCK_DGRAM) # this seemingly useless code avoids awkward lockup under Windows XP

fix_broken_imports()

###############################################################################

class ModuleLoaderError(Exception): pass
class InvalidModuleNameError(ModuleLoaderError): pass
class ModuleNotFoundError(ModuleLoaderError): pass
class ModuleReloadTimedOutError(ModuleLoaderError): pass
class ModuleAccessTimedOutError(ModuleLoaderError): pass
class ModuleFileBrokenError(ModuleLoaderError): pass
class ModuleFileIncompleteError(ModuleLoaderError): pass
class ModuleWithDependenciesError(ModuleLoaderError): pass
class ModuleAlreadyImportedError(ModuleLoaderError): pass
class ModuleNotImportedError(ModuleLoaderError): pass
class InvalidMethodAccessError(ModuleLoaderError): pass

###############################################################################

valid_node_name = by_regex("^[A-Za-z0-9_-]{1,32}$")
valid_cage_name = by_regex("^(?:[A-Za-z0-9_-]{1,32}|\\.shared)$")
valid_cage_name_suffix = by_regex("^(?:[A-Za-z0-9_-]{1,32}(?::retry|:reverse)?|(?::retry|:reverse))$")
valid_module_ext = by_regex("^\\.pyc?$")

###############################################################################

class MethodProxy:

    def __init__(self, method, unlock_module, src_module):
        self._method, self._unlock_module = method, unlock_module
        self._src_module = src_module

    def __del__(self):
        self.__dict__.pop("_unlock_module", lambda: None)()

    def __call__(self, *args, **kwargs):
        try:
            if self._src_module is not None:
                kwargs["__source_module_name"] = self._src_module
            return self._method(*args, **kwargs)
        finally:
            self.__dict__.pop("_unlock_module")()

###############################################################################

class Module:

    def __init__(self, name, loader):
        self._name, self._loader = name, loader
        self._module, self._reloadable = None, True
        self._ts, self._ts_timeout = None, Timeout(1.0)
        self._sh_lock = SharedLockWriterPriority("pmnc.{0:s}".format(name))
        self._lock, self._attrs = Lock(), {}

    def _acquire(self, request):
        unlock = self._sh_lock.release
        if not request.acquire(self._sh_lock):
            raise ModuleReloadTimedOutError("request deadline waiting for exclusive "
                                            "access to module {0:s}".format(self._name))
        return unlock

    def acquire_shared(self, request):
        unlock = self._sh_lock.release_shared
        if not request.acquire_shared_fast(self._sh_lock):
            raise ModuleAccessTimedOutError("request deadline waiting for shared "
                                            "access to module {0:s}".format(self._name))
        return unlock

    # this method is called from module proxy to locate a method or a class,
    # the information about once located objects is cached to avoid costly introspection
    # in getfullargspec, which is used for determining whether or not the method
    # requires a special kwarg __source_module_name in which the name of the calling
    # module is passed

    def get_attr_info(self, name):

        if not self._module:
            raise ModuleNotImportedError("module {0:s} has not been loaded".\
                                         format(self._name))
        if name.startswith("_"):
            raise InvalidMethodAccessError("attribute {0:s} should be private to module "
                                           "{1:s}".format(name, self._name))
        if name not in self._module.__all__:
            raise InvalidMethodAccessError("attribute {0:s} is not declared in __all__ "
                                           "list of module {1:s}".format(name, self._name))
        with self._lock:
            attr_info = self._attrs.get(name)
            if not attr_info:
                attr = getattr(self._module, name)
                if isfunction(attr):
                    requires_src_module_kwarg = "__source_module_name" in getfullargspec(attr).kwonlyargs
                    attr_info = (typecheck(attr), requires_src_module_kwarg)
                elif isclass(attr):
                    def create_object(*args, **kwargs):
                        instance = attr(*args, **kwargs)
                        setattr(instance, "__containing_module__", self._module)
                        return instance
                    create_object.__name__ = attr.__name__
                    attr_info = (create_object, False)
                else:
                    raise InvalidMethodAccessError("attribute {0:s} in module {1:s} is neither "
                                                   "a class nor a function".format(name, self._name))
                self._attrs[name] = attr_info

        return attr_info

    # this method extracts the file modification time
    # and is only called as often as _ts_timeout permits

    @staticmethod
    def _get_file_ts(filename):
        try:
            return stat(filename).st_mtime
        except:
            return 0

    # note that the following method is interlocked and possibly
    # returns True to only one thread per file update

    def _requires_reload(self, filename):
        with self._lock:
            if self._reloadable and (self._ts is None or self._ts_timeout.expired):
                self._ts, ts = self._get_file_ts(filename), self._ts or 0
                self._ts_timeout.reset()
                return ts < self._ts
            else:
                return False

    # the thread lucky to be performing this reload thus has just one
    # chance to do it, if it fails, a file update will be required to
    # cause a reload again

    def _reload(self, filename, request):

        re = self._module is not None and "re" or ""

        self._loader.log.message("{0:s}loading module {1:s} from {2:s}".\
                                 format(re, self._name, filename))
        try:

            # managing imports requires holding a global lock

            acquire_imp_lock()
            try:

                if self._name in sys_modules:
                    raise ModuleAlreadyImportedError("module {0:s} has already been "
                                                     "imported".format(self._name))

                with open(filename, "rb") as module:

                    ext = os_path.splitext(filename)[1]
                    assert valid_module_ext(ext)

                    if ext == ".py":

                        # as a simple guard against picking up incomplete files,
                        # being simultaneously written to, we require the modules
                        # to end with # EOF

                        try:
                            next(filter(lambda s: s.rstrip() == b"# EOF", module))
                        except StopIteration:
                            raise ModuleFileIncompleteError("file {0:s} is incomplete, does not "
                                                            "end with # EOF".format(filename))
                        else:
                            module.seek(0) # rewind the file

                    # actually import the module, it can already be broken
                    # again at this point, but we don't care

                    try:
                        load_module(self._name, module, filename, ("", "rb", ext == ".py" and PY_SOURCE or PY_COMPILED))
                    except Exception as e:
                        raise ModuleFileBrokenError("file {0:s} is broken: {1:s}".format(filename, e))

                # the pmnc-accessible modules are invisible in sys.modules

                module = sys_modules.pop(self._name)

                # see if the loaded module has __all__ attribute,
                # and if not provide a default empty list

                all = getattr(module, "__all__", None)
                if all is None:
                    setattr(module, "__all__", [])
                    self._loader.log.warning("module {0:s} has no __all__ attribute and will "
                                             "export no methods or classes".format(self._name))
                else:
                    assert list_of(str)(all), "__all__ attribute must be a list of strings"

                # append self_test method to a list of accessible
                # methods for the module being tested

                if request.self_test == self._name:
                    all.append("self_test")

            finally:
                release_imp_lock()

            # the module has been successfully loaded from the file

            try:

                # the newly imported module should have just one reference to it

                if getrefcount(module) != 2:
                    raise ModuleWithDependenciesError("the newly loaded module {0:s} has "
                                                      "unexpected dependencies".format(self._name))

                # a module containing __reloadable__ = False is assumed
                # to have state and hence be not reloadable

                reloadable = bool(getattr(module, "__reloadable__", True))

                # the imported module is instrumented with pmnc and others

                setattr(module, "pmnc", ModuleLoaderProxy(self._loader, self._name))

                setattr(module, "__node__", self._loader._node_name)
                setattr(module, "__cage__", self._loader._cage_name)
                setattr(module, "__module__", self._name)
                setattr(module, "__cage_dir__", self._loader._cage_directory)

                # success, the methods cache is cleared and the previous version is discarded

                with self._lock:
                    self._attrs.clear()

                self._module, module, self._reloadable = module, self._module, reloadable

            finally:
                del module

        except ModuleLoaderError as e:
            if self._module is not None:
                self._loader.log.message("reloading of module {0:s} failed: {1:s} (the error "
                                         "is ignored)".format(self._name, str(e)))
            else:
                raise
        else:
            self._loader.log.message("module {0:s} has been {1:s}loaded{2:s}".format(self._name, re,
                                     not reloadable and " (not reloadable)" or ""))

    def reload(self, filename, request, src_module):

        # reload the module if it hasn't been loaded before, or reload is required

        if self._requires_reload(filename) or self._module is None:
            unlock_module = self._acquire(request)
            try:
                self._reload(filename, request)
            finally:
                unlock_module()

        # either way proceed to the actual call

        unlock_module = self.acquire_shared(request)
        try:
            module_proxy = ModuleProxy(self, request, unlock_module, src_module)
        except:
            unlock_module()
            raise
        else:
            return module_proxy

###############################################################################

class ModuleProxy:

    def __init__(self, module, request, unlock_module, src_module):
        self._module, self._request = module, request
        self._unlock_module, self._src_module = unlock_module, src_module

    def __del__(self):
        self.__dict__.pop("_unlock_module", lambda: None)()

    def __getattr__(self, name):
        try:
            unlock_module = self._module.acquire_shared(self._request)
            try:
                attr, requires_src_module_kwarg = self._module.get_attr_info(name)
                src_module = requires_src_module_kwarg and self._src_module or None
                method_proxy = MethodProxy(attr, unlock_module, src_module)
            except:
                unlock_module()
                raise
            else:
                return method_proxy
        finally:
            self.__dict__.pop("_unlock_module")()

###############################################################################

class ModuleLoader:

    @typecheck
    def __init__(self, node_name: valid_node_name, cage_name: valid_cage_name,
                 cage_directory: os_path.isdir, log: callable, log_priority: int):
        self._node_name, self._cage_name = node_name, cage_name
        self._log, self._log_priority = log, log_priority
        self._cage_directory = cage_directory
        self._module_locator = ModuleLocator(self._cage_directory)
        self._lock, self._modules, self._loggers = Lock(), {}, {}

    ###################################

    class _Logger:

        def __init__(self, log, log_priority, src_module):
            self._log, self._log_priority = log, log_priority
            self._src_module = src_module

        def __call__(self, s, priority = None):
            try:
                if priority is None:
                    return self(s, priority = 4)
                elif priority <= self._log_priority:
                    while not isinstance(s, str):
                        try:
                            s = str(s)
                        except:
                            s = str(s.__class__)
                    request = getattr(current_thread(), "_request", None)
                    req_desc = request.description if request else ""
                    req_desc = " by {0:s}".format(req_desc) if req_desc else ""
                    if self._src_module:
                        line, func = extract_stack(None, 3)[-3][1:3]
                        s += " # {0:s}.py:{1:d} in {2:s}(){3:s}".\
                             format(self._src_module, line, func, req_desc)
                    elif req_desc:
                        s += " #{0:s}".format(req_desc)
                    self._log(s, priority = priority)
            except:
                pass # do nothing

        error = lambda self, s: self(s, priority = 1)
        message = lambda self, s: self(s, priority = 2)
        warning = lambda self, s: self(s, priority = 3)
        info = lambda self, s: self(s, priority = 5)
        debug = lambda self, s: self(s, priority = 6)

    def set_log_priority(self, log_priority):
        with self._lock:
            if self._log_priority != log_priority:
                self._log_priority = log_priority
                self._loggers.clear()

    ###################################

    def __getattr__(self, module_name, src_module = None):

        # special case #1: pmnc.log[.error|.message|.warning|.info|.debug](s)
        # I prefer to cache logger instances created for each module for two
        # reasons: (1) runtime exception safety and (2) performance

        if module_name == "log":
            with self._lock:
                logger = self._loggers.get(src_module)
                if logger is None:
                    logger = ModuleLoader._Logger(self._log, self._log_priority, src_module)
                    self._loggers[src_module] = logger
            return logger

        # special case #2: pmnc.request

        request = current_thread()._request

        if module_name == "request":
            return request

        # reserve a private namespace for modules

        if module_name.startswith("_"):
            raise InvalidModuleNameError("module name cannot start with underscore")

        # locate the file containing the required module

        module_filename = \
            self._module_locator.locate("{0:s}.py".format(module_name)) or \
            self._module_locator.locate("{0:s}.pyc".format(module_name))

        if module_filename is None:
            raise ModuleNotFoundError("file {0:s}.py was not found".format(module_name))

        # see if such module has already been loaded, create an empty object if it hasn't

        with self._lock:
            module = self._modules.get(module_name)
            if not module:
                module = Module(module_name, self)
                self._modules[module_name] = module

        # reload the module if necessary and return the proxy object

        return module.reload(module_filename, request, src_module)

###############################################################################

class ModuleLoaderProxy:

    def __init__(self, loader, module_name):
        self._loader, self._module_name = loader, module_name

    def __getattr__(self, module_name):
        return self._loader.__getattr__(module_name, self._module_name)

    # this method initially handles syntax for RPC calls
    # pmnc(["other_cage", ]option = value).module.method(*args, **kwargs)
    #                                    ^ the method is invoked here

    def __call__(self, cage_name: optional(valid_cage_name_suffix) = None, **options):
        return RemoteModuleLoader(self._loader, cage_name or "", options)

###############################################################################

class RemoteModuleLoader:

    def __init__(self, module_loader, cage_name, options):
        self._loader, self._cage_name, self._options = module_loader, cage_name, options

    def __getattr__(self, name):
        return RemoteModuleProxy(self._loader, self._cage_name, self._options, name)

###############################################################################

class RemoteModuleProxy:

    @typecheck
    def __init__(self, module_loader, cage_name, options, module_name):
        self._loader, self._cage_name = module_loader, cage_name
        self._options, self._module_name = options, module_name

    def __getattr__(self, name):
        return RemoteMethodProxy(self._loader, self._cage_name,
                                 self._options, self._module_name, name)

###############################################################################

class RemoteMethodProxy:

    def __init__(self, module_loader, cage_name, options, module_name, method_name):
        self._loader, self._cage_name, self._options = module_loader, cage_name, options
        self._module_name, self._method_name = module_name, method_name

    def __call__(self, *args, **kwargs):
        if self._cage_name.endswith(":retry"):
            cage_name = self._cage_name[:-6] or self._loader._cage_name
            return self._loader.remote_call.execute_async(
                            cage_name, self._module_name, self._method_name,
                            args, kwargs, **self._options)
        elif self._cage_name.endswith(":reverse"):
            cage_name = self._cage_name[:-8]; assert cage_name
            return self._loader.reverse_call.execute_reverse(
                            cage_name, self._module_name, self._method_name,
                            args, kwargs, **self._options)
        elif "queue" in self._options:
            cage_name = self._cage_name or self._loader._cage_name
            return self._loader.remote_call.execute_async(
                            cage_name, self._module_name, self._method_name,
                            args, kwargs, **self._options)
        else:
            return self._loader.remote_call.execute_sync(
                            self._cage_name, self._module_name, self._method_name,
                            args, kwargs, **self._options)

###############################################################################

if __name__ == "__main__":

    print("self-testing module module_loader.py:")

    from expected import expected
    from tempfile import mkdtemp
    from os import mkdir, remove, listdir, rename
    from shutil import rmtree
    from time import sleep
    from threading import Event, Thread
    from sys import executable as python
    from subprocess import Popen
    from pmnc.request import InfiniteRequest, fake_request

    ###################################

    assert valid_cage_name_suffix("foo")
    assert valid_cage_name_suffix("foo:retry")
    assert valid_cage_name_suffix("foo:reverse")
    assert valid_cage_name_suffix("Foo_Bar-2")
    assert valid_cage_name_suffix("Foo_Bar-2:retry")
    assert valid_cage_name_suffix("Foo_Bar-2:reverse")
    assert valid_cage_name_suffix(":retry")
    assert valid_cage_name_suffix(":reverse")

    assert not valid_cage_name_suffix("")
    assert not valid_cage_name_suffix("foo:bar")
    assert not valid_cage_name_suffix(":bar")

    ###################################

    node_name = "node"
    cage_name = "cage"

    ###################################

    log_lock = Lock()
    log_lines = []

    def log(s, *, priority):
        with log_lock:
            log_lines.append(s)

    ###################################

    cages_dir = mkdtemp()
    default_dir = os_path.join(cages_dir, ".shared")
    mkdir(default_dir)
    cage_dir = os_path.join(cages_dir, cage_name)
    mkdir(cage_dir)

    ###################################

    def write_module(name, contents, encoding = "windows-1251"):
        sleep(2.0)
        with open(os_path.join(cage_dir, name), "wb") as f:
            f.write(contents.encode(encoding))

    ###################################

    pmnc = ModuleLoader(node_name, cage_name, cage_dir, log, 6)

    ###################################

    print("simple module reload: ", end = "")

    fake_request(30.0)

    write_module(os_path.join("..", ".shared", "foo.py"),
                 "__all__ = ['get_version']\n"
                 "def get_version():\n"
                 "    return 1\n"
                 "# EOF")

    assert pmnc.foo.get_version() == 1

    write_module("foo.py",
                 "__all__ = ['get_version']\n"
                 "def get_version():\n"
                 "    return 2\n"
                 "# EOF")

    assert pmnc.foo.get_version() == 2

    write_module(os_path.join("..", ".shared", "foo.py"),
                 "__all__ = ['get_version']\n"
                 "def get_version():\n"
                 "    return 'ignored'\n"
                 "# EOF")

    assert pmnc.foo.get_version() == 2

    write_module("foo.py",
                 "__all__ = ['get_version']\n"
                 "def get_version():\n"
                 "    return 3\n"
                 "# EOF")

    assert pmnc.foo.get_version() == 3

    remove(os_path.join(cage_dir, "foo.py"))

    sleep(1.5)
    assert pmnc.foo.get_version() == 3

    write_module(os_path.join("..", ".shared", "foo.py"),
                 "__all__ = ['get_version']\n"
                 "def get_version():\n"
                 "    return 4\n"
                 "# EOF")

    assert pmnc.foo.get_version() == 4

    print("ok")

    ###################################

    print("international characters: ", end = "")

    fake_request(30.0)

    rus = "\u0410\u0411\u0412\u0413\u0414\u0415\u0401\u0416\u0417\u0418\u0419" \
          "\u041a\u041b\u041c\u041d\u041e\u041f\u0420\u0421\u0422\u0423\u0424" \
          "\u0425\u0426\u0427\u0428\u0429\u042c\u042b\u042a\u042d\u042e\u042f" \
          "\u0430\u0431\u0432\u0433\u0434\u0435\u0451\u0436\u0437\u0438\u0439" \
          "\u043a\u043b\u043c\u043d\u043e\u043f\u0440\u0441\u0442\u0443\u0444" \
          "\u0445\u0446\u0447\u0448\u0449\u044c\u044b\u044a\u044d\u044e\u044f"

    write_module(os_path.join("..", ".shared", "rus.py"),
                 "#!/usr/bin/env python\n"
                 "#-*- coding: cp866 -*-\n"
                 "__all__ = ['get_rus']\n"
                 "rus = '" + rus + "'\n"
                 "def get_rus():\n"
                 "    return rus\n"
                 "# EOF", "cp866")

    assert pmnc.rus.get_rus() == rus

    print("ok")

    ###################################

    print("compiled module: ", end = "")

    fake_request(30.0)

    py_name = os_path.join(cage_dir, "pyc.py")
    pyc_name = os_path.join(cage_dir, "pyc.pyc")

    write_module(py_name,
                 "__all__ = ['get_name']\n"
                 "def get_name():\n"
                 "    return __name__\n"
                 "# (NO LONGER NEEDED) EOF")

    Popen([ python, "-c", "import pyc" ], cwd = cage_dir).wait()

    try:
        from imp import get_tag
    except ImportError:
        pass
    else:
        pycache_name = os_path.join(cage_dir, "__pycache__", "pyc.{0:s}.pyc".format(get_tag()))
        assert os_path.isfile(pycache_name)
        rename(pycache_name, pyc_name)
        assert not os_path.isfile(pycache_name)

    remove(py_name)
    assert not os_path.isfile(py_name)

    assert os_path.isfile(pyc_name)
    assert pmnc.pyc.get_name() == "pyc"

    print("ok")

    ###################################

    print("module reload timeout: ", end = "")

    fake_request(0.1)
    sleep(0.5)

    write_module(os_path.join("..", ".shared", "reload_timeout.py"),
                 "__all__ = ['foo']\n"
                 "def foo():\n"
                 "    return 1\n"
                 "# EOF")

    with expected(ModuleReloadTimedOutError("request deadline waiting for exclusive access to module reload_timeout")):
        pmnc.reload_timeout.foo()

    fake_request(3.0)

    assert pmnc.reload_timeout.foo() == 1

    print("ok")

    ###################################

    print("__all__ declaration: ", end = "")

    fake_request(30.0)

    write_module(os_path.join("..", ".shared", "all_test.py"),
                 "def inaccessible(): pass\n"
                 "class Inaccessible(): pass\n"
                 "# EOF")

    with expected(InvalidMethodAccessError("attribute inaccessible is not declared "
                                           "in __all__ list of module all_test")):
        pmnc.all_test.inaccessible()

    with expected(InvalidMethodAccessError("attribute Inaccessible is not declared "
                                           "in __all__ list of module all_test")):
        pmnc.all_test.Inaccessible()

    sleep(1.5)
    write_module(os_path.join("..", ".shared", "all_test.py"),
                 "__all__ = ['foo']\n"
                 "def inaccessible2(): pass\n"
                 "class Inaccessible2(): pass\n"
                 "# EOF")

    with expected(InvalidMethodAccessError("attribute inaccessible2 is not declared "
                                           "in __all__ list of module all_test")):
        pmnc.all_test.inaccessible2()

    with expected(InvalidMethodAccessError("attribute Inaccessible2 is not declared "
                                           "in __all__ list of module all_test")):
        pmnc.all_test.Inaccessible2()

    sleep(1.5)
    write_module(os_path.join("..", ".shared", "all_test.py"),
                 "__all__ = ['accessible', 'Accessible']\n"
                 "def accessible(): pass\n"
                 "class Accessible(): pass\n"
                 "# EOF")

    pmnc.all_test.accessible()
    pmnc.all_test.Accessible()

    sleep(1.5)
    write_module(os_path.join("..", ".shared", "all_test.py"),
                 "__all__ = [1]\n"
                 "# EOF")

    with expected(AssertionError("__all__ attribute must be a list of strings")):
        pmnc.all_test

    print("ok")

    ###################################

    print("one module loads another and vice versa: ", end = "")

    fake_request(30.0)

    write_module(os_path.join("..", ".shared", "re_reload_1.py"),
                 "__all__ = ['f', 'g']\n"
                 "result = None\n"
                 "def f():\n"
                 "    global result\n"
                 "    result = 'ok'\n"
                 "    return pmnc.re_reload_2.h()\n"
                 "def g():\n"
                 "    return result\n"
                 "# EOF")

    write_module(os_path.join("..", ".shared", "re_reload_2.py"),
                 "__all__ = ['h']\n"
                 "def h():\n"
                 "    return pmnc.re_reload_1.g()\n"
                 "# EOF")

    assert pmnc.re_reload_1.f() == "ok"

    print("ok")

    ###################################

    print("remote cage calls: ", end = "")

    fake_request(30.0)

    write_module("remote_call.py",
                 "__all__ = ['execute_sync', 'execute_async', 'test_sync',\n"
                 "           'test_async_1', 'test_async_2', 'test_async_3', 'test_async_4']\n"
                 "def execute_sync(cage, module, method, args, kwargs, **options):\n"
                 "    return 'sync', cage, module, method, args, kwargs, options\n"
                 "def execute_async(cage, module, method, args, kwargs, **options):\n"
                 "    return 'async', cage, module, method, args, kwargs, options\n"
                 "def test_sync(*args, **kwargs):\n"
                 "    return pmnc('sync_cage', opt_1 = 'aaa').foo.bar(*args, **kwargs)\n"
                 "def test_async_1(*args, **kwargs):\n"
                 "    return pmnc('async_cage_1:retry', opt_2 = 'bbb').biz.baz(*args, **kwargs)\n"
                 "def test_async_2(*args, **kwargs):\n"
                 "    return pmnc('async_cage_2', queue = 'queue', opt_3 = 'ccc').tic.tac(*args, **kwargs)\n"
                 "def test_async_3(*args, **kwargs):\n"
                 "    return pmnc(':retry', opt_4 = 'ddd').zip.zap(*args, **kwargs)\n"
                 "def test_async_4(*args, **kwargs):\n"
                 "    return pmnc(queue = 'queue', opt_5 = 'eee').abc.cba(*args, **kwargs)\n"
                 "# EOF")

    assert pmnc.remote_call.test_sync(1, "2", foo = "bar") == \
           ("sync", "sync_cage", "foo", "bar", (1, "2"), {"foo": "bar"}, {"opt_1": "aaa"})

    assert pmnc.remote_call.test_async_1(3, "4", biz = "baz") == \
           ("async", "async_cage_1", "biz", "baz", (3, "4"), {"biz": "baz"}, {"opt_2": "bbb"})

    assert pmnc.remote_call.test_async_2(5, "6", ppp = "vvv") == \
           ("async", "async_cage_2", "tic", "tac", (5, "6"), {"ppp": "vvv"}, {"queue": "queue", "opt_3": "ccc"})

    assert pmnc.remote_call.test_async_3(7, "8", sss = "ttt") == \
           ("async", "cage", "zip", "zap", (7, "8"), {"sss": "ttt"}, {"opt_4": "ddd"})

    assert pmnc.remote_call.test_async_4(9, "10", ggg = "hhh") == \
           ("async", "cage", "abc", "cba", (9, "10"), {"ggg": "hhh"}, {"queue": "queue", "opt_5": "eee"})

    write_module("reverse_call.py",
                 "__all__ = ['execute_reverse', 'test_reverse']\n"
                 "def execute_reverse(cage, module, method, args, kwargs, **options):\n"
                 "    return 'reverse', cage, module, method, args, kwargs, options\n"
                 "def test_reverse(*args, **kwargs):\n"
                 "    return pmnc('reverse_cage:reverse', opt_6 = 'fff').ping.pong(*args, **kwargs)\n"
                 "# EOF")

    assert pmnc.reverse_call.test_reverse(11, "12", qqq = "rrr") == \
           ("reverse", "reverse_cage", "ping", "pong", (11, "12"), {"qqq": "rrr"}, {"opt_6": "fff"})

    print("ok")

    ###################################

    print("sys modules can't be reloaded: ", end = "")

    fake_request(30.0)

    write_module("time.py",
                 "# EOF")

    with expected(ModuleAlreadyImportedError):
        pmnc.time

    print("ok")

    ###################################

    print("modules can be marked as not reloadable: ", end = "")

    fake_request(30.0)

    write_module("stateful.py",
                 "__reloadable__ = False\n"
                 "__all__ = ['get_version']\n"
                 "def get_version():\n"
                 "    return 1\n"
                 "# EOF")

    assert pmnc.stateful.get_version() == 1

    write_module("stateful.py",
                 "__all__ = ['get_version']\n"
                 "def get_version():\n"
                 "    return 2\n"
                 "# EOF")

    assert pmnc.stateful.get_version() == 1

    print("ok")

    ###################################

    print("class instance lifetime: ", end = "")

    fake_request(30.0)

    write_module("instance.py",
                 "__all__ = ['SomeClass', 'get_version']\n"
                 "class SomeClass:\n"
                 "    def __init__(self, *args, **kwargs):\n"
                 "        self._args, self._kwargs = args, kwargs\n"
                 "    def get_init_args(self):\n"
                 "        return self._args, self._kwargs\n"
                 "    def get_class_version(self):\n"
                 "        return 'A'\n"
                 "    def get_static_module_version(self):\n"
                 "        return get_version()\n"
                 "    def get_dynamic_module_version(self):\n"
                 "        return pmnc.instance.get_version()\n"
                 "def get_version():\n"
                 "    return 1\n"
                 "# EOF")

    sc = pmnc.instance.SomeClass("foo", "bar", biz = "baz")
    assert pmnc.instance.get_version() == 1
    assert sc.get_init_args() == (("foo", "bar"), {"biz": "baz"})
    assert sc.get_static_module_version() == 1
    assert sc.get_dynamic_module_version() == 1
    assert sc.get_class_version() == 'A'

    write_module("instance.py",
                 "__all__ = ['SomeClass', 'get_version']\n"
                 "class SomeClass:\n"
                 "    def __init__(self, *args, **kwargs):\n"
                 "        self._args, self._kwargs = args, kwargs\n"
                 "    def get_init_args(self):\n"
                 "        return self._args, self._kwargs\n"
                 "    def get_class_version(self):\n"
                 "        return 'B'\n"
                 "    def get_static_module_version(self):\n"
                 "        return get_version()\n"
                 "    def get_dynamic_module_version(self):\n"
                 "        return pmnc.instance.get_version()\n"
                 "def get_version():\n"
                 "    return 2\n"
                 "# EOF")

    sc2 = pmnc.instance.SomeClass("foo2", "bar2", biz2 = "baz2")
    assert pmnc.instance.get_version() == 2
    assert sc2.get_init_args() == (("foo2", "bar2"), {"biz2": "baz2"})
    assert sc2.get_static_module_version() == 2
    assert sc2.get_dynamic_module_version() == 2
    assert sc2.get_class_version() == 'B'

    # now let's check the old instance

    assert sc.get_init_args() == (("foo", "bar"), {"biz": "baz"})
    assert sc.get_static_module_version() == 1
    assert sc.get_dynamic_module_version() == 2 # note the change
    assert sc.get_class_version() == 'A'

    print("ok")

    ###################################

    print("module instrumentation: ", end = "")

    r = fake_request(30.0)

    write_module("whoami.py",
                 "__all__ = ['info', 'my_req']\n"
                 "def info():\n"
                 "    return __node__, __cage__, __module__, __cage_dir__\n"
                 "def my_req():\n"
                 "    return pmnc.request\n"
                 "# EOF")

    assert pmnc.whoami.info() == ("node", "cage", "whoami", cage_dir)
    assert pmnc.whoami.my_req() is r

    write_module("logging.py",
                 "#!/usr/bin/env python\n"
                 "#-*- coding: utf-8 -*-\n"
                 "__all__ = ['test']\n"
                 "def test():\n"
                 "    pmnc.log.error('ERROR')\n"
                 "    pmnc.log.message('MESSAGE')\n"
                 "    pmnc.log.warning('WARNING')\n"
                 "    pmnc.log('DEFAULT')\n"
                 "    pmnc.log.info('INFO')\n"
                 "    pmnc.log.debug('DEBUG')\n"
                 "# EOF")

    del log_lines[:]
    pmnc.logging.test()
    assert log_lines[-6:] == \
           [ "ERROR # logging.py:5 in test() by {0:s}".format(r.description),
             "MESSAGE # logging.py:6 in test() by {0:s}".format(r.description),
             "WARNING # logging.py:7 in test() by {0:s}".format(r.description),
             "DEFAULT # logging.py:8 in test() by {0:s}".format(r.description),
             "INFO # logging.py:9 in test() by {0:s}".format(r.description),
             "DEBUG # logging.py:10 in test() by {0:s}".format(r.description) ]

    pmnc.set_log_priority(1)

    del log_lines[:]
    pmnc.logging.test()
    assert log_lines == [ "ERROR # logging.py:5 in test() by {0:s}".format(r.description) ]

    pmnc.set_log_priority(6)

    print("ok")

    ###################################

    print("module names such as _this are reserved: ", end = "")

    fake_request(30.0)

    with expected(InvalidModuleNameError("module name cannot start with underscore")):
        pmnc._foo

    print("ok")

    ###################################

    print("private methods are inaccessible: ", end = "")

    fake_request(30.0)

    write_module("hideme.py",
                 "def _foo():\n"
                 "    pass\n"
                 "# EOF")

    with expected(InvalidMethodAccessError("attribute _foo should be private to module hideme")):
        pmnc.hideme._foo

    print("ok")

    ###################################

    print("one module calls another: ", end = "")

    fake_request(30.0)

    write_module("biz.py",
                 "__all__ = ['call_baz']\n"
                 "def call_baz():\n"
                 "    return pmnc.baz.whoareyou()\n"
                 "# EOF")

    write_module("baz.py",
                 "__all__ = ['whoareyou']\n"
                 "def whoareyou():\n"
                 "    return 'module baz'\n"
                 "# EOF")

    assert pmnc.biz.call_baz() == "module baz"

    print("ok")

    ###################################

    print("two modules exchange calls: ", end = "")

    fake_request(30.0)

    write_module("biz.py",
                 "__all__ = ['f']\n"
                 "def f(n):\n"
                 "    if n == 1: return 1\n"
                 "    return n * pmnc.baz.f(n - 1)\n"
                 "# EOF")

    write_module("baz.py",
                 "__all__ = ['f']\n"
                 "def f(n):\n"
                 "    if n == 1: return 1\n"
                 "    return n * pmnc.biz.f(n - 1)\n"
                 "# EOF")

    assert pmnc.biz.f(50) == 30414093201713378043612608166064768844377641568960512000000000000

    print("ok")

    ###################################

    print("broken module: ", end = "")

    fake_request(30.0)

    write_module("broken.py",
                 "***\n"
                 "# EOF")

    with expected(ModuleFileBrokenError):
        pmnc.broken

    write_module("broken.py",
                 "parses = ok")

    with expected(ModuleFileIncompleteError):
        pmnc.broken

    write_module("broken.py",
                 "__all__ = ['some_func']\n"
                 "def some_func():\n"
                 "    return 'foo'\n"
                 "# EOF")

    assert pmnc.broken.some_func() == "foo"

    write_module("broken.py",
                 "***\n"
                 "# EOF")

    assert pmnc.broken.some_func() == "foo"

    # same but with indirect call

    write_module("caller.py",
                 "__all__ = ['foo']\n"
                 "def foo():\n"
                 "    return pmnc.callee.bar()\n"
                 "# EOF")

    write_module("callee.py",
                 "__all__ = ['bar']\n"
                 "def bar():\n"
                 "    *whoops*\n"
                 "# EOF")

    with expected(ModuleFileBrokenError):
        pmnc.caller.foo()

    write_module("callee.py",
                 "__all__ = ['bar']\n"
                 "def bar():\n"
                 "    return 'ok'\n"
                 "# EOF")

    assert pmnc.caller.foo() == "ok"

    with expected(ModuleNotFoundError("file notthere.py was not found")):
        pmnc.notthere

    print("ok")

    ###################################

    print("source-module-aware module: ", end = "")

    fake_request(30.0)

    write_module("state.py",
                 "__all__ = ['get']\n"
                 "def get(*, __source_module_name):\n"
                 "    return 'state_' + __source_module_name\n"
                 "# EOF")

    write_module("foo.py",
                 "__all__ = ['get_state']\n"
                 "def get_state():\n"
                 "    return pmnc.state.get()\n"
                 "# EOF")

    assert pmnc.foo.get_state() == 'state_foo';

    write_module("bar.py",
                 "__all__ = ['get_state']\n"
                 "def get_state():\n"
                 "    return pmnc.state.get()\n"
                 "# EOF")

    assert pmnc.bar.get_state() == 'state_bar';

    print("ok")

    ###################################

    print("attribute problems: ", end = "")

    fake_request(30.0)

    write_module("foo.py",
                 "__all__ = ['notthere', 'biz', 're_Foo', 're_Foo2', 'Foo']\n"
                 "biz = 1\n"
                 "def re_Foo():\n"
                 "    global Foo\n"
                 "    Foo = lambda: 'replaced'\n"
                 "def re_Foo2():\n"
                 "    global Foo\n"
                 "    Foo = lambda: 'already replaced'\n"
                 "\n"
                 "# EOF")

    with expected(AttributeError("'module' object has no attribute 'notthere'")):
        pmnc.foo.notthere

    with expected(InvalidMethodAccessError("attribute biz in module foo is neither a class nor a function")):
        pmnc.foo.biz

    pmnc.foo.re_Foo()
    assert pmnc.foo.Foo() == "replaced"

    pmnc.foo.re_Foo2()
    assert pmnc.foo.Foo() == "replaced" # the reference is cached

    print("ok")

    ###################################

    print("threaded access to a module: ", end = "")

    fake_request(30.0)

    write_module("wait_set.py",
                 "__all__ = ['wait', 'set']\n"
                 "def wait(e):\n"
                 "    e.wait()\n"
                 "def set(e):\n"
                 "    e.set()\n"
                 "# EOF")

    e = Event()

    th1 = Thread(target = lambda: pmnc.wait_set.wait(e))
    th1.daemon = 1; th1._request = InfiniteRequest();

    th2 = Thread(target = lambda: pmnc.wait_set.set(e))
    th2.daemon = 1; th2._request = InfiniteRequest();

    th1.start()
    th2.start()

    th1.join(1.0)
    th2.join(1.0)

    assert not th1.is_alive() and not th2.is_alive()

    print("ok")

    ###################################

    sleep(1.0)
    rmtree(cages_dir)

    ###################################

    print("all ok")

###############################################################################
# EOF
