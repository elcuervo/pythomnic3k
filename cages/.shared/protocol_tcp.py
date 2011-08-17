#!/usr/bin/env python
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module contains an implementation of a generic TCP-based request-response
# interface, it is used internally as a base for the specific application-level
# protocol interfaces, such as HTTP, and should not therefore be used directly,
# but through protocol-specific descendants, such as protocol_http.py.
#
# The module also contains a generic "resource" for sending/receiving data
# over TCP. Because it respects the request timeout and does not block, it is
# used in many other TCP-based resources as a low level send/receive facility.
#
# Both interface and resource support SSL as well as plain TCP.
#
# Pythomnic3k project
# (c) 2005-2010, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "TcpInterface", "TcpResource" ]

###############################################################################

import threading; from threading import Event, Lock, current_thread
import select; from select import select, error as select_error
import socket; from socket import socket, AF_INET, SOCK_STREAM, SOCK_DGRAM, \
                                  SOL_SOCKET, SO_REUSEADDR, error as socket_error
import io; from io import BytesIO
import os; from os import SEEK_CUR, path as os_path
import ssl; from ssl import wrap_socket, PROTOCOL_SSLv23, CERT_OPTIONAL, CERT_REQUIRED, \
                            CERT_NONE, SSLError, SSL_ERROR_WANT_WRITE, SSL_ERROR_WANT_READ
import random; from random import shuffle

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, callable, optional, one_of
import interlocked_queue; from interlocked_queue import InterlockedQueue
import pmnc.timeout; from pmnc.timeout import Timeout
import pmnc.threads; from pmnc.threads import HeavyThread
import pmnc.request; from pmnc.request import Request

###############################################################################

# this utility function returns a description
# of a connection for a given socket

def _cf(socket): # connection from
    try:
        return "connection from {0[0]:s}:{0[1]:d}".\
               format(socket.getpeername())
    except:
        return "aborted connection" # should not throw

###############################################################################

# this utility function extracts common name
# field from ssl peer's certificate

def _peer_cn(s):
    cert = s.getpeercert()
    if cert:
        for field in cert["subject"]:
            if field[0][0] == "commonName":
                return field[0][1]
    return None

###############################################################################

class TcpConnection:
    """
    <TcpConnection>
    An instance of this class is created for each TCP connection accepted by
    the interface's listener thread. The I/O thread invokes that instance's
    methods in response to network events such as data available on the socket.
    The actual protocol specific processing is done by a new instance of handler
    for each new request, created and owned by this instance. The TCP connection
    is kept alive while handler allows.
    """

    @typecheck
    def __init__(self, interface_name: str, socket, handler_factory: callable,
                 request_timeout: float):

        self._interface_name = interface_name
        self._socket, self._cf = socket, _cf(socket)
        self._peer_ip = socket.getpeername()[0]
        self._handler_factory = handler_factory
        self._handler = None
        self._request_timeout = request_timeout
        self._go_active()
        self._state = self._state_initialize
        self._unwritten_data = None

    socket = property(lambda self: self._socket)
    expired = property(lambda self: self._timeout.expired)
    request = property(lambda self: self._request)
    idle = property(lambda self: self._idle)

    ###################################

    def _go_idle(self, idle_timeout: float):
        self._idle = True
        self._timeout = Timeout(idle_timeout)
        self._request = None

    def _go_active(self):
        self._idle = False
        self._timeout = Timeout(self._request_timeout)
        self._handler = self._handler_factory(self._handler)
        self._request = None

    ###################################

    """
    Each time the handler finishes parsing a network protocol request, a new
    Pythomnic request is created and started. Similarly, whenever this connection
    instance is discarded, either upon success or failure, the request is ended.
    """

    def _request_parameters(self):                              # these are made separate methods to allow
        return dict(auth_tokens = dict(peer_ip = self._peer_ip, # overriding them in SslConnection class
                                       encrypted = False))
    def _describe_request(self):
        self._request.describe("TCP connection from {0:s}".format(self._peer_ip))

    def _begin_request(self):
        self._request = pmnc.interfaces.begin_request(
                    timeout = self._timeout.remain, interface = self._interface_name,
                    protocol = self._handler.protocol, parameters = self._request_parameters())
        self._describe_request()

    def _end_request(self, reason = None):
        if self._request:
            request, self._request = self._request, None
            pmnc.interfaces.end_request(reason is None, request)

    ###################################

    def _state_initialize(self):                 # this fake state performs the initial
        self._state = self._state_read           # dispatch of the connection just accepted
        return "read"

    ###################################

    def _recv(self):
        return self._socket.recv(4096), "read"

    def _state_read(self):
        data, wait_state = self._recv()
        if data:                                 # some data received from the client
            if self._idle:
                self._go_active()
            if self._handler.consume(data):
                self._begin_request()
                self._state = self._state_resume
                return "enqueue"
            else:                                # otherwise, keep reading
                return wait_state
        elif self._idle:                         # it's ok for the client to disconnect
            self._state = None                   # between requests
            return "close"
        else:
            raise Exception("unexpected eof")

    ###################################

    """
    The actual processing of the parsed request is performed by the same handler
    that parsed it (nobody else would know how to handle it). The process_tcp_request
    method is invoked by one of the main thread pool threads from wu_process_tcp_request
    in I/O thread.
    """

    def process_tcp_request(self):
        self._handler.process_tcp_request()

    ###################################

    def _state_resume(self):                     # this fake state performs the dispatch
        self._state = self._state_write          # of the connection with ready response
        return "write"

    ###################################

    def _send(self, data):
        return self._socket.send(data)

    def _state_write(self):
        data = self._unwritten_data or \
               self._handler.produce(4096)       # the handler gives out a response
        if data:                                 # some data still needs to be sent
            try:
                sent = self._send(data)
            except:
                self._unwritten_data = data      # this allows the send attempt to be retried
                raise                            # if appropriate (see SslConnection class)
            else:
                self._unwritten_data = None
            if sent > 0:
                unsent = len(data) - sent
                if unsent > 0:
                    self._handler.retract(unsent)
                return "write"
            else:
                raise Exception("unexpected eof")
        else:
            self._end_request()
            idle_timeout = self._handler.idle_timeout
            if idle_timeout > 0.0:               # if the connection is kept alive
                self._go_idle(idle_timeout)      # return to reading another request
                self._state = self._state_read
                return "read"
            else:                                # gracefully close the connection
                self._state = None
                return "close"

    ###################################

    def process(self):                           # this is a state switch
        return self._state()

    ###################################

    def close(self, reason):                     # this is called from I/O thread's
        self._end_request(reason)                # discard_socket procedure
        try:
            self._socket.close()
        except:
            pmnc.log.error(exc_string()) # log and ignore

###############################################################################

class SslConnection(TcpConnection):
    """
    <SslConnection>
    This class is a functionally equivalent descendant of TcpConnection.
    It has a handful of methods overriden to support SSL transport.
    """

    @typecheck
    def __init__(self, interface_name: str, socket, handler_factory: callable, request_timeout: float,
                 ssl_key_cert_file: os_path.isfile, ssl_ca_cert_file: os_path.isfile,
                 required_auth_level: one_of(CERT_REQUIRED, CERT_OPTIONAL, CERT_NONE)):

        TcpConnection.__init__(self, interface_name, socket, handler_factory, request_timeout)

        self._tcp_socket = self._socket
        self._socket = wrap_socket(self._tcp_socket, server_side = True,
                                   ssl_version = PROTOCOL_SSLv23, cert_reqs = required_auth_level,
                                   keyfile = ssl_key_cert_file, certfile = ssl_key_cert_file,
                                   ca_certs = ssl_ca_cert_file, do_handshake_on_connect = False)
        self._socket.setblocking(False)

    def _state_initialize(self):                 # this fake state performs the initial
        self._state = self._state_handshake      # dispatch of the connection just accepted
        return "read"

    def _state_handshake(self):
        self._socket.do_handshake()
        self._peer_cn = _peer_cn(self._socket)   # this can be None if peer provided no certificate
        if self._peer_cn:
            pmnc.log.debug("{0:s} is encrypted ({1:s}) and authenticated ({2:s})".\
                            format(self._cf, self._socket.cipher()[0], self._peer_cn))
        else:
            pmnc.log.debug("{0:s} is encrypted ({1:s})".\
                            format(self._cf, self._socket.cipher()[0]))
        self._state = self._state_read
        return "read"

    def _request_parameters(self):
        request_parameters = TcpConnection._request_parameters(self)
        auth_tokens = request_parameters["auth_tokens"]
        auth_tokens.update(encrypted = True)
        if self._peer_cn is not None:
            auth_tokens.update(peer_cn = self._peer_cn)
        return request_parameters

    def _describe_request(self):
        self._request.describe("SSL connection from {0:s}".format(self._peer_ip))

    # receiving SSL data in non-blocking mode is ugly; for one, more data could
    # have been received and buffered by the SSL socket than we may want to read,
    # hence we have to keep reading while it is possible; for two, even though
    # we are only reading, key renegotiation may occur which needs to write, hence
    # WANT_WRITE case is possible; for three, we may encounter EOF while reading,
    # and there is no way we can signal it to the caller without possibly losing
    # the data already read; finally, we may not be reading infinitely but we have
    # to protect ourselves against flooding by setting an arbitrary upper bound

    _max_read_data = 1048576

    def _recv(self):
        data = b""
        while len(data) <= self._max_read_data: # prevent flooding, because the data read here is
            try:                                # not seen by the handler in the course of reading
                portion = self._socket.read(4096)
                if portion:
                    data += portion
                else: # EOF from the client
                    wait_state = "read"
                    break
            except SSLError as e:
                if data: # if no data has been read, the exception is rethrown
                    if e.args[0] == SSL_ERROR_WANT_READ:
                        wait_state = "read"
                        break
                    elif e.args[0] == SSL_ERROR_WANT_WRITE:
                        wait_state = "write"
                        break
                raise
        else:
            raise Exception("input size exceeded")
        return data, wait_state

    def _send(self, data):
        return self._socket.write(data)

    def process(self):                           # this is a state switch
        try:
            return self._state()
        except SSLError as e:                    # connection state remains the same,
            if e.args[0] == SSL_ERROR_WANT_READ: # but the I/O wait state switches
                return "read"
            elif e.args[0] == SSL_ERROR_WANT_WRITE:
                return "write"
            else:
                raise

###############################################################################

class TcpInterface: # called such so as not to be confused with "real" Interface's
    """
    <TcpInterface>
    This is a generic facility for implementing request-response protocols
    over TCP or SSL. An instance of this class owns two threads - listener
    thread and I/O thread. Listener thread accepts incoming TCP connections
    and hands them over to the I/O thread. I/O thread performs all the network
    processing in asynchronous mode (via select). The actual protocol details
    are processed by the handlers created by the supplied handler_factory.
    """

    @typecheck
    def __init__(self,
                 name: str, # the name of the "real" interface
                 handler_factory: callable,
                 request_timeout: float,
                 *,
                 listener_address: (str, int),
                 max_connections: int,
                 ssl_key_cert_file: optional(os_path.isfile),
                 ssl_ca_cert_file: optional(os_path.isfile),
                 required_auth_level: optional(one_of(CERT_REQUIRED, CERT_OPTIONAL, CERT_NONE)) = CERT_OPTIONAL):

        self._name = name
        self._handler_factory = handler_factory
        self._listener_address = listener_address
        self._max_connections = max_connections

        assert (ssl_key_cert_file is None and ssl_ca_cert_file is None) or \
               (os_path.isfile(ssl_key_cert_file) and os_path.isfile(ssl_ca_cert_file)), \
               "both certificate files must be specified or none of them"

        self._ssl_key_cert_file = ssl_key_cert_file
        self._ssl_ca_cert_file = ssl_ca_cert_file
        self._use_ssl = ssl_key_cert_file is not None and ssl_ca_cert_file is not None

        # create two sides of a UDP socket used for kicking
        # I/O thread from blocking select

        self._pulse_recv = socket(AF_INET, SOCK_DGRAM)
        self._pulse_recv.bind(("127.0.0.1", 0))
        self._pulse_recv_address = self._pulse_recv.getsockname()

        self._pulse_send_lock = Lock()
        self._pulse_send = socket(AF_INET, SOCK_DGRAM)
        self._pulse_send.bind((self._pulse_recv_address[0], 0))

        # create a queue used for delivering sockets to I/O thread

        self._socket_queue = InterlockedQueue()

        # select the appropriate class for connections

        if self._use_ssl:
            self._connection_factory = \
                lambda socket: SslConnection(self._name, socket, self._handler_factory, request_timeout,
                                             self._ssl_key_cert_file, self._ssl_ca_cert_file,
                                             required_auth_level)
        else:
            self._connection_factory = \
                lambda socket: TcpConnection(self._name, socket, self._handler_factory, request_timeout)

        # this Event when set prevents the I/O thread
        # from enqueueing more requests

        self._ceased = Event()

    name = property(lambda self: self._name)
    listener_address = property(lambda self: self._listener_address)
    encrypted = property(lambda self: self._use_ssl)

    ###################################

    def start(self):

        # create and start the I/O thread

        self._io = HeavyThread(target = self._io_proc,
                               name = "{0:s}:i/o".format(self._name))
        self._io.start()

        try: # create and start the listener thread

            started_listening = Event()
            self._listener = HeavyThread(target = self._listener_proc,
                                         name = "{0:s}:lsn".format(self._name),
                                         args = (started_listening, ))
            self._listener.start()

            # wait for the listener thread to actually start listening

            try:
                started_listening.wait(3.0) # this may spend waiting slightly less, but it's ok
                if not started_listening.is_set():
                    raise Exception("failed to start listening")
            except:
                self.cease()
                raise

        except:
            self.stop()
            raise

    ###################################

    def cease(self):
        self._ceased.set()    # this prevents the I/O thread from introducing new requests
        self._listener.stop() # (from keep-alives) and the listener thread is simply stopped

    ###################################

    def stop(self):
        self._io.stop()

    ###################################

    def _push_socket_wake_io(self, socket, mode):

        self._socket_queue.push((socket, mode))

        # kick the I/O thread from its blocking select to hasten this socket processing

        with self._pulse_send_lock:
            self._pulse_send.sendto(b"\x00", 0, self._pulse_recv_address)

    ###################################

    # this method is a work unit executed by one of the interface pool threads,
    # it does the actual processing and resubmits the socket back to the I/O thread
    # for sending the response back to the client

    def wu_process_tcp_request(self, socket, connection):

        # see for how long the request was on the execution queue up to this moment
        # and whether it has expired in the meantime, if it did there is no reason
        # to proceed and we simply bail out

        if pmnc.request.expired:
            pmnc.log.error("request has expired and will not be processed")
            return

        try:
            with pmnc.performance.request_processing():
                connection.process_tcp_request()
            self._push_socket_wake_io(socket, "reuse")
        except:
            pmnc.log.error(exc_string()) # log and ignore, this catches only interface failures
                                         # if this happens, there is noone to help anyway

    ###################################

    # this single thread multiplexes all the I/O on the interface

    def _io_proc(self):

        # all the currently active connections are here, arranged in select-ready lists,
        # alongside with the mapping of sockets to the protocol-specific connections

        r_sockets, w_sockets, connections = [self._pulse_recv], [], {}

        # this function unconditionally removes all traces of a socket,
        # it is the last resort and presumably should not throw

        def discard_socket(socket, reason = None):
            try:
                w_sockets.remove(socket)     # the socket may be in
            except ValueError:               # |
                try:                         # V
                    r_sockets.remove(socket) # at most one list
                except ValueError:
                    pass
            connection = connections.pop(socket, None)
            if reason:
                if connection and connection.idle:
                    pmnc.log.debug("gracefully closing {0:s} ({1:s})".format(_cf(socket), reason))
                else:
                    pmnc.log.error("discarding {0:s}: {1:s}".format(_cf(socket), reason))
            else:
                pmnc.log.debug("gracefully closing {0:s}".format(_cf(socket)))
            if connection:
                try:
                    connection.close(reason)
                except:
                    pmnc.log.error(exc_string()) # log and ignore
            try:
                socket.close()
            except:
                pmnc.log.error(exc_string()) # log and ignore

        # this function is called periodically and forcefully discards the sockets
        # whose connections have expired, having this 3 seconds slack also helps in
        # gracefully delivering the "deadline expired" kind of responses although
        # this is random and there is no guarantee such responses will be delivered

        discard_timeout = Timeout(pmnc.request.self_test != "protocol_tcp" and 3.0 or 1.0)

        def discard_expired_sockets():
            if discard_timeout.expired:
                try:
                    for socket, connection in list(connections.items()): # need to create a copy of connections
                        if connection.expired:
                            discard_socket(socket, "connection expired")
                finally:
                    discard_timeout.reset()

        # this function is called once at shutdown to terminate all idle
        # persistent connections to prevent them from introducing more requests

        idle_sockets_discarded = False

        def discard_idle_sockets():
            nonlocal idle_sockets_discarded
            try:
                for socket, connection in list(connections.items()): # need to create a copy of connections
                    if connection.idle:
                        discard_socket(socket, "interface shutdown")
            finally:
                idle_sockets_discarded = True

        # this function selects sockets that can be read or written,
        # also dealing with possible select failures

        def select_rw_sockets(timeout):
            while not timeout.expired:
                try:
                    return select(r_sockets, w_sockets, [], timeout.remain)[0:2]
                except (select_error, ValueError):
                    rs = []
                    for r_socket in r_sockets[:]: # need to create a copy of r_sockets
                        try:
                            if select([r_socket], [], [], 0.0)[0]:
                                rs.append(r_socket)
                        except (select_error, ValueError):
                            discard_socket(r_socket, "select failure")
                            continue # for
                    ws = []
                    for w_socket in w_sockets[:]: # need to create a copy of w_sockets
                        try:
                            if select([], [w_socket], [], 0.0)[1]:
                                ws.append(w_socket)
                        except (select_error, ValueError):
                            discard_socket(w_socket, "select failure")
                            continue # for
                    if rs or ws:
                        return rs, ws
            else:
                return [], []

        # this function passes control to the given socket's connection,
        # then dispatches it to the appropriate wait list

        def process_socket(socket, sockets = None):
            try:
                connection = connections.get(socket)
                if not connection: # must have already been discarded
                    return
                wait_state = connection.process()
                if wait_state == "read":
                    if sockets is w_sockets:
                        w_sockets.remove(socket)
                    if sockets is not r_sockets:
                        r_sockets.append(socket)
                elif wait_state == "write":
                    if sockets is r_sockets:
                        r_sockets.remove(socket)
                    if sockets is not w_sockets:
                        w_sockets.append(socket)
                elif wait_state == "enqueue":
                    if sockets is not None:
                        sockets.remove(socket)
                    if self._ceased.is_set():
                        discard_socket(socket, "interface shutdown")
                    else:
                        self._enqueue_request(connection.request,
                                              self.wu_process_tcp_request,
                                              (socket, connection), {})
                elif wait_state == "close":
                    discard_socket(socket)
                else:
                    assert False, "invalid wait state"
            except:
                discard_socket(socket, exc_string())

        # this function creates a connection for a newly accepted
        # socket and puts it to the appropriate wait list

        def create_connection(socket):
            if len(connections) < self._max_connections:
                try:
                    connection = self._connection_factory(socket)
                    socket = connection.socket # this may be not the original TCP socket
                    connections[socket] = connection
                    process_socket(socket) # this does the appropriate initial dispatch
                except:
                    discard_socket(socket, exc_string())
            else:
                discard_socket(socket, "too many connections")

        # this function puts the socket for the processed
        # request to the appropriate wait list

        def reuse_connection(socket):
            try:
                process_socket(socket) # this does the appropriate dispatch upon resume
            except:
                discard_socket(socket, exc_string())

        # this function reads and discards all UDP packets
        # received and buffered on the pulse socket

        def drain_pulse_socket():
            while select([self._pulse_recv], [], [], 0.0)[0]:
                try:
                    assert self._pulse_recv.recv(1) == b"\x00"
                except:
                    pmnc.log.error(exc_string()) # log and ignore

        # this thread multiplexes all the I/O on all the client sockets

        while not current_thread().stopped(): # lifetime loop
            try:

                discard_expired_sockets()

                if self._ceased.is_set() and not idle_sockets_discarded:
                    discard_idle_sockets()

                # select the sockets which are ready for I/O and process them

                readable_sockets, writable_sockets = select_rw_sockets(Timeout(1.0))

                for r_socket in readable_sockets:
                    if r_socket is self._pulse_recv: # special case
                        drain_pulse_socket()
                    else:
                        process_socket(r_socket, r_sockets)

                for w_socket in writable_sockets:
                    process_socket(w_socket, w_sockets)

                # check for new incoming sockets from the queue

                u_socket, mode = self._socket_queue.pop(0.0) or (None, None)
                while u_socket:
                    try:
                        if mode == "create":
                            create_connection(u_socket)
                        elif mode == "reuse":
                            reuse_connection(u_socket)
                        else:
                            assert False, "invalid mode"
                    except:
                        discard_socket(u_socket, exc_string())
                    u_socket, mode = self._socket_queue.pop(0.0) or (None, None)

            except:
                pmnc.log.error(exc_string()) # log and ignore

        # discard the remaining sockets

        for socket, connection in list(connections.items()): # need to create a copy of connections
            discard_socket(socket, "interface shutdown")

    ###################################

    # by default requests are enqueued to the main thread pool,
    # but this method can be overridden to use a different pool,
    # this is is done by RPC interface for example

    def _enqueue_request(self, *args, **kwargs):
        pmnc.interfaces.enqueue(*args, **kwargs)

    ###################################

    def _create_random_server_socket(self) -> socket:

        # negative port specifies random port range, for example -12000 means 12000-12999

        server_address, base_random_port = self._listener_address
        low_port = -base_random_port

        for scale in (10000, 1000, 100, 10, 1):
            if low_port % scale == 0:
                high_port = low_port + scale
                break

        ports = list(range(low_port, high_port))
        shuffle(ports)

        for port in ports:
            server_socket = socket(AF_INET, SOCK_STREAM)
            try:
                server_socket.bind((server_address, port)) # note the absence of SO_REUSEADDR
            except socket_error:
                server_socket.close()
            else:
                return server_socket
        else:
            raise Exception("ran out of possible bind ports")

    ###################################

    def _create_static_server_socket(self) -> socket:

        server_socket = socket(AF_INET, SOCK_STREAM)
        try:
            server_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            server_socket.bind(self._listener_address)
        except:
            server_socket.close()
            raise
        else:
            return server_socket

    ###################################

    def _listener_proc(self, started_listening):

        # bind the socket and start listening

        try:
            server_port = self._listener_address[1]
            if server_port >= 0:
                server_socket = self._create_static_server_socket()
            else:
                server_socket = self._create_random_server_socket()
            self._listener_address = server_socket.getsockname()
            server_socket.listen(256)
        except:
            pmnc.log.error(exc_string())
            return

        try:

            started_listening.set()
            pmnc.log.message("started listening for connections at {0[0]:s}:{0[1]:d}".\
                             format(self._listener_address))

            # this thread keeps accepting incoming connections and
            # pushing the new sockets to the i/o thread's queue

            while not current_thread().stopped(): # lifetime loop
                try:
                    if select([server_socket], [], [], 1.0)[0]:
                        client_socket, client_address = server_socket.accept()
                        pmnc.log.debug("incoming connection from {0[0]:s}:{0[1]:d}".\
                                       format(client_address))
                        client_socket.setblocking(False)
                        self._push_socket_wake_io(client_socket, "create")
                except:
                    pmnc.log.error(exc_string()) # log and ignore

        finally:
            server_socket.close()

        pmnc.log.message("stopped listening")

###############################################################################

class TcpResource: # called such so as not to be confused with "real" Resource's
    """
    <TcpResource>
    This is a generic facility for implementing client protocol sides
    over TCP or SSL. In the course of sending or receiving data this class
    respects the request timeout and is not blocking.
    """

    @typecheck
    def __init__(self, name: str, *,
                 server_address: (str, int),
                 connect_timeout: float,
                 ssl_key_cert_file: optional(os_path.isfile),
                 ssl_ca_cert_file: optional(os_path.isfile)):

        self._name = name
        self._server_address = server_address
        self._connect_timeout = connect_timeout

        assert ssl_key_cert_file is None or os_path.isfile(ssl_ca_cert_file), \
               "CA certificate file must be specified"

        self._ssl_key_cert_file = ssl_key_cert_file
        self._ssl_ca_cert_file = ssl_ca_cert_file

        self._use_ssl = ssl_ca_cert_file is not None
        self._peer_cn = None
        self._server_info = "{0:s} {1:s}://{2[0]:s}:{2[1]:d}".\
                            format(self._name, self._use_ssl and "ssl" or "tcp",
                                   self._server_address)

    name = property(lambda self: self._name)
    encrypted = property(lambda self: self._use_ssl)
    peer_cn = property(lambda self: self._peer_cn)
    server_info = property(lambda self: self._server_info)

    ###################################

    # TCP-specific send/recv

    def _tcp_send(self, data):
        if not select([], [self._socket], [], pmnc.request.remain)[1]:
            raise Exception("request deadline writing data to {0:s}".format(self._server_info))
        return self._socket.send(data)

    def _tcp_recv(self, n):
        if not select([self._socket], [], [], pmnc.request.remain)[0]:
            raise Exception("request deadline reading data from {0:s}".format(self._server_info))
        return self._socket.recv(n)

    ###################################

    # SSL-specific send/recv

    def _ssl_retry(self, label, timeout, f):
        while not timeout.expired:
            try:
                return f()
            except SSLError as e:
                if e.args[0] == SSL_ERROR_WANT_READ:
                    select([self._socket], [], [], timeout.remain)
                elif e.args[0] == SSL_ERROR_WANT_WRITE:
                    select([], [self._socket], [], timeout.remain)
                else:
                    raise
        else:
            raise Exception("request deadline {0:s} {1:s}".\
                            format(label, self._server_info))

    def _ssl_send(self, data):
        return self._ssl_retry("writing data to", pmnc.request,
                               lambda: self._socket.write(data))

    def _ssl_recv(self, n):
        return self._ssl_retry("reading data from", pmnc.request,
                               lambda: self._socket.read(n))

    ###################################

    def connect(self):

        pmnc.log.debug("connecting to {0:s}".format(self._server_info))
        try:

            self._socket = socket(AF_INET, SOCK_STREAM)
            try:

                # establish TCP connection within connect_timeout
                # or request timeout whichever is smaller

                timeout = Timeout(min(self._connect_timeout, pmnc.request.remain))

                self._socket.settimeout(timeout.remain or 0.01)
                self._socket.connect(self._server_address)
                self._socket.setblocking(False)

                if self._use_ssl: # perform SSL handshake

                    self._socket.getpeername() # fixme - remove, see http://bugs.python.org/issue4171

                    self._tcp_socket = self._socket
                    self._socket = wrap_socket(self._tcp_socket, server_side = False, ssl_version = PROTOCOL_SSLv23,
                                               keyfile = self._ssl_key_cert_file, certfile = self._ssl_key_cert_file,
                                               ca_certs = self._ssl_ca_cert_file, cert_reqs = CERT_REQUIRED,
                                               do_handshake_on_connect = False)
                    self._socket.setblocking(False)

                    # perform asynchronous handshake within the rest of connect_timeout

                    self._ssl_retry("waiting for handshake with", timeout, self._socket.do_handshake)

                    # extract peer's certificate info, there has to be one, because we use CERT_REQUIRED

                    self._peer_cn = _peer_cn(self._socket)
                    self._server_info = "{0:s} ssl://{1[0]:s}:{1[1]:d}/cn={2:s}".\
                                        format(self._name, self._server_address, self._peer_cn)

                    # select appropriate send/recv methods

                    self._send = self._ssl_send
                    self._recv = self._ssl_recv

                else:

                    self._send = self._tcp_send
                    self._recv = self._tcp_recv

            except:
                self._socket.close()
                raise

        except:
            pmnc.log.error("connection to {0:s} failed: {1:s}".\
                           format(self._server_info, exc_string()))
            raise
        else:
            pmnc.log.debug("connected to {0:s}".format(self._server_info))

    ###################################

    def send_request(self, request: bytes, response_handler: optional(callable) = None):

        if request:
            pmnc.log.debug("sending {0:d} byte(s) to {1:s}".\
                           format(len(request), self._server_info))

        # send request, occasionally timing out

        request = BytesIO(request)
        data = request.read(4096)
        while data:
            sent = self._send(data)
            if sent == 0:
                raise Exception("unexpected eof writing data to {0:s}".\
                                format(self._server_info))
            unsent = len(data) - sent
            if unsent > 0:
                request.seek(-unsent, SEEK_CUR)
            data = request.read(4096)

        if response_handler is None:
            return None

        pmnc.log.debug("waiting for response from {0:s}".format(self._server_info))

        # receive response, occasionally timing out

        response_length = 0
        while True:
            data = self._recv(4096)
            response_length += len(data)
            response = response_handler(data) # notify response handler before checking for eof,
            if response is not None:          # because eof may be legitimate at this time
                break
            if not data:
                raise Exception("unexpected eof reading data from {0:s}".\
                                format(self._server_info))

        pmnc.log.debug("received {0:d} byte(s) from {1:s}".\
                       format(response_length, self._server_info))

        return response

    ###################################

    def disconnect(self):
        try:
            pmnc.log.debug("disconnecting from {0:s}".format(self._server_info))
            self._socket.close()
        except:
            pmnc.log.error(exc_string()) # log and ignore

###############################################################################

def self_test():

    from time import time, sleep
    from random import random, randint
    from threading import Thread
    from os import urandom
    from pmnc.request import fake_request

    request_timeout = pmnc.config_interfaces.get("request_timeout")
    max_connections = pmnc.config_interfaces.get("thread_count") * 2

    ###################################

    def test_interface(ssl_key_cert_file, ssl_ca_cert_file):

        ###############################

        def start_interface(handler_factory, **kwargs):
            kwargs.setdefault("listener_address", ("127.0.0.1", 0))
            kwargs.setdefault("max_connections", max_connections)
            kwargs.setdefault("ssl_key_cert_file", ssl_key_cert_file)
            kwargs.setdefault("ssl_ca_cert_file", ssl_ca_cert_file)
            ifc = TcpInterface("test", handler_factory, request_timeout, **kwargs)
            ifc.start()
            return ifc

        ###############################

        def connect_to(ifc): # blocking connect
            s = socket(AF_INET, SOCK_STREAM)
            s.connect(ifc.listener_address)
            if ssl_key_cert_file:
                s = wrap_socket(s, cert_reqs = CERT_REQUIRED, ssl_version = PROTOCOL_SSLv23,
                                ca_certs = ssl_ca_cert_file, do_handshake_on_connect = False)
                try:
                    s.do_handshake()
                except:
                    pmnc.log.error(exc_string())
            return s

        ###############################

        def drain_socket(socket, timeout):
            t = Timeout(timeout)
            if select([socket], [], [], t.remain)[0]:
                result = b""
                while True:
                    try:
                        result += socket.recv(1024)
                    except:
                        pmnc.log.error(exc_string())
                        break
                    else:
                        if t.expired or not select([socket], [], [], t.remain)[0]:
                            break
                return result
            else:
                return None

        ###############################

        def peer_drops_connection(socket, timeout):
            t = Timeout(timeout)
            while not t.expired:
                if drain_socket(socket, 0.1) == b"":
                    return True
            return False

        ###############################

        class NullParser:
            def __init__(self, prev_handler):
                pass

        ###############################

        class LineEchoParser:
            protocol = "line_echo"
            def __init__(self, prev_handler):
                self._data = b""
            def consume(self, data):
                self._data += data
                if self._data.endswith(b"\n"):
                    return True
            def process_tcp_request(self):
                self._response = BytesIO(self._data)
                del self._data # as this is a one-request handler
            def produce(self, n):
                return self._response.read(n)
            def retract(self, n):
                self._response.seek(-n, SEEK_CUR)
            idle_timeout = 5.0

        ###############################

        pmnc.log.message("****************** START/STOP ******************")

        def test_start_stop():
            ifc = start_interface(NullParser)
            try:
                assert ifc.listener_address[0] == "127.0.0.1"
                sleep(2.0) # to let the threads run
            finally:
                ifc.cease(); ifc.stop()

        test_start_stop()

        ###############################

        pmnc.log.message("****************** START/STOP RANDOM PORT ******************")

        def test_start_stop_random_port():
            ifc = start_interface(NullParser, listener_address = ("127.0.0.1", -54300))
            try:
                assert ifc.listener_address[0] == "127.0.0.1"
                assert 54300 <= ifc.listener_address[1] < 54400
                sleep(2.0) # to let the threads run
            finally:
                ifc.cease(); ifc.stop()

        test_start_stop_random_port()

        ###############################

        pmnc.log.message("****************** CONNECT/DISCONNECT ******************")

        def test_connect_disconnect():
            ifc = start_interface(NullParser)
            try:
                s = connect_to(ifc)
                sleep(2.0)
                s.close()
            finally:
                ifc.cease(); ifc.stop()

        test_connect_disconnect()

        ###############################

        pmnc.log.message("****************** CONNECTION LIMIT ******************")

        def test_max_connections():
            ifc = start_interface(NullParser)
            try:
                ss = []
                for i in range(max_connections):
                    ss.append(connect_to(ifc))
                    sleep(0.1)
                for s in ss:
                    assert not peer_drops_connection(s, 0.1)
                s = connect_to(ifc)
                assert peer_drops_connection(s, 1.0)
            finally:
                ifc.cease(); ifc.stop()

        test_max_connections()

        ###############################

        pmnc.log.message("****************** SIMPLE REQUEST/RESPONSE TEST ******************")

        def test_process_request_response():
            ifc = start_interface(LineEchoParser)
            try:
                s = connect_to(ifc)
                r = s.makefile("rb")
                for i in range(1, 19):
                    data = urandom(randint(2 ** (i - 1), 2 ** i)).\
                           replace(b"\n", b" ").replace(b"\r", b" ").replace(b"\x00", b" ") + b"\n"
                    s.sendall(data)
                    resp = r.readline()
                    assert resp == data
            finally:
                ifc.cease(); ifc.stop()

        test_process_request_response()

        ###############################

        pmnc.log.message("****************** LOOPBACK CONNECTION FAILURE TEST ******************")

        def test_loopback_connection_failure():
            ifc = start_interface(LineEchoParser)
            try:

                fake_request(10.0)

                # connection failure

                r = pmnc.protocol_tcp.TcpResource("tres1", server_address = ("1.2.3.4", 1234),
                                                  connect_timeout = 3.0,
                                                  ssl_key_cert_file = ifc._ssl_key_cert_file,
                                                  ssl_ca_cert_file = ifc._ssl_ca_cert_file)
                try:
                    r.connect()
                except:
                    pmnc.log.error(exc_string())
                else:
                    assert False, "shouldn't be able to connect"

            finally:
                ifc.cease(); ifc.stop()

        test_loopback_connection_failure()

        ###############################

        def test_res(ifc, r):

            assert r.encrypted == (ifc._ssl_key_cert_file is not None)
            assert r.peer_cn is None

            r.connect()
            try:

                assert r.encrypted == (ifc._ssl_key_cert_file is not None)
                if r.encrypted:
                    assert r.peer_cn is not None

                response = b""

                def loopback_handler(b):
                    nonlocal response
                    response += b
                    if response.endswith(b"\n"):
                        return response

                for i in range(1, 19):
                    data = urandom(randint(2 ** (i - 1), 2 ** i)).\
                           replace(b"\n", b" ").replace(b"\r", b" ").replace(b"\x00", b" ") + b"\n"
                    response = b""
                    assert r.send_request(data, loopback_handler) == data

            finally:
                r.disconnect()

        ###############################

        pmnc.log.message("****************** LOOPBACK 1-WAY TEST ******************")

        def test_loopback_1way():
            ifc = start_interface(LineEchoParser)
            try:

                fake_request(10.0)

                r = pmnc.protocol_tcp.TcpResource("tres2", server_address = ifc.listener_address,
                                                  connect_timeout = 3.0, ssl_key_cert_file = None,
                                                  ssl_ca_cert_file = ifc._ssl_ca_cert_file)
                test_res(ifc, r)

            finally:
                ifc.cease(); ifc.stop()

        test_loopback_1way()

        ###############################

        pmnc.log.message("****************** LOOPBACK 2-WAY TEST ******************")

        def test_loopback_2way():
            ifc = start_interface(LineEchoParser, required_auth_level = CERT_REQUIRED)
            try:

                fake_request(10.0)

                if ifc.encrypted:
                    r = pmnc.protocol_tcp.TcpResource("tres2", server_address = ifc.listener_address,
                                                      connect_timeout = 3.0, ssl_key_cert_file = None,
                                                      ssl_ca_cert_file = ifc._ssl_ca_cert_file)
                    try:
                        r.connect()
                    except:
                        pass
                    else:
                        assert False, "shouldn't be able to connect without a client certificate"

                r = pmnc.protocol_tcp.TcpResource("tres2", server_address = ifc.listener_address,
                                                  connect_timeout = 3.0,
                                                  ssl_key_cert_file = ifc._ssl_key_cert_file,
                                                  ssl_ca_cert_file = ifc._ssl_ca_cert_file)

                test_res(ifc, r)

            finally:
                ifc.cease(); ifc.stop()

        test_loopback_2way()

        ###############################

        pmnc.log.message("****************** INITIAL IDLE TIMEOUT ******************")

        def test_initial_idle_timeout():
            ifc = start_interface(NullParser)
            try:
                s = connect_to(ifc)
                assert not peer_drops_connection(s, request_timeout - 1.0)
                assert peer_drops_connection(s, 3.0)
            finally:
                ifc.cease(); ifc.stop()

        test_initial_idle_timeout()

        ###############################

        pmnc.log.message("****************** REQUEST READING TIMEOUT ******************")

        def test_read_timeout():
            ifc = start_interface(LineEchoParser)
            try:
                s = connect_to(ifc)
                t = Timeout(request_timeout - 1.0)
                while not t.expired:
                    s.sendall(b"x")
                    sleep(0.1)
                assert peer_drops_connection(s, 3.0)
            finally:
                ifc.cease(); ifc.stop()

        test_read_timeout()

        ###############################

        pmnc.log.message("****************** PROCESSING TIMEOUT ******************")

        class SlowProcessingLineEchoParser(LineEchoParser):
            def process_tcp_request(self):
                sleep(request_timeout + 1.0)
                self._response = BytesIO(self._data)

        def test_process_timeout():
            ifc = start_interface(SlowProcessingLineEchoParser)
            try:
                s = connect_to(ifc)
                s.sendall(b"x\n")
                assert not peer_drops_connection(s, request_timeout - 1.0)
                assert peer_drops_connection(s, 3.0)
            finally:
                ifc.cease(); ifc.stop()

        test_process_timeout()

        ###############################

        pmnc.log.message("****************** PROCESSING ERROR ******************")

        class SlowProcessingLineEchoParser(LineEchoParser):
            def process_tcp_request(self):
                raise Exception("processing error")

        def test_process_error():
            ifc = start_interface(SlowProcessingLineEchoParser)
            try:
                s = connect_to(ifc)
                s.sendall(b"x\n")
                assert not peer_drops_connection(s, request_timeout - 1.0)
                assert peer_drops_connection(s, 3.0)
            finally:
                ifc.cease(); ifc.stop()

        test_process_error()

        ###############################

        pmnc.log.message("****************** RESPONSE WRITING TIMEOUT ******************")

        class SlowWritingLineEchoParser(LineEchoParser):
            def produce(self, n):
                sleep(0.1)
                return self._response.read(1)

        def test_write_timeout():
            ifc = start_interface(SlowWritingLineEchoParser)
            try:
                s = connect_to(ifc)
                t = Timeout(request_timeout - 1.0)
                s.sendall(b"x" * 1000 + b"\n")
                while t.remain > 1.0:
                    x = s.recv(10)
                    assert x == b"x" * len(x)
                assert not peer_drops_connection(s, t.remain)
                assert peer_drops_connection(s, 4.0)
            finally:
                ifc.cease(); ifc.stop()

        test_write_timeout()

        ###############################

        pmnc.log.message("****************** KEEP-ALIVE TIMEOUT ******************")

        def test_reuse_timeout():
            ifc = start_interface(LineEchoParser)
            try:
                s = connect_to(ifc)
                s.sendall(b"foo\n")
                assert peer_drops_connection(s, 7.0)
            finally:
                ifc.cease(); ifc.stop()

        test_reuse_timeout()

        ###############################

        pmnc.log.message("****************** TWO KEEP-ALIVE TIMEOUTS ******************")

        def test_reuse_timeout2():
            ifc = start_interface(LineEchoParser)
            try:
                s = connect_to(ifc)
                s.sendall(b"foo\n") # request #1
                assert not peer_drops_connection(s, 4.0) # 5.0 - 1.0
                s.sendall(b"foo\n") # request #2
                assert not peer_drops_connection(s, 4.0)
                assert peer_drops_connection(s, 3.0)
            finally:
                ifc.cease(); ifc.stop()

        test_reuse_timeout2()

        ###############################

        pmnc.log.message("****************** STRESS TEST ******************")

        class RandomProcessingLineEchoParser(LineEchoParser):
            def process_tcp_request(self):
                sleep(random() * 0.2)
                LineEchoParser.process_tcp_request(self)

        def test_stress():
            ifc = start_interface(RandomProcessingLineEchoParser)
            try:

                requests_per_thread = 50
                request_count_lock = Lock()
                request_count = 0

                def th_proc():
                    s = connect_to(ifc)
                    r = s.makefile("rb")
                    for i in range(requests_per_thread):
                        sleep(random() * 0.2)
                        data = b"x" * randint(1, 262144) + b"\n"
                        s.sendall(data)
                        sleep(random() * 0.2)
                        assert r.readline() == data
                        with request_count_lock:
                            nonlocal request_count
                            request_count += 1
                    assert not peer_drops_connection(s, 4.5)
                    assert peer_drops_connection(s, 4.5)

                ths = [ Thread(target = th_proc) for i in range(max_connections) ]
                for th in ths: th.start()
                for th in ths: th.join()

                assert request_count == requests_per_thread * max_connections

            finally:
                ifc.cease(); ifc.stop()

        test_stress()

        ###############################

        pmnc.log.message("****************** LOCKOUT TEST ******************")

        class HangingLineEchoParser(LineEchoParser):
            def process_tcp_request(self):
                if self._data != b"ok\n":
                    sleep(request_timeout + 1.0)
                LineEchoParser.process_tcp_request(self)

        def test_lockout():
            ifc = start_interface(HangingLineEchoParser)
            try:

                requests_per_thread = 10

                def th_proc():
                    s = connect_to(ifc)
                    r = s.makefile("rb")
                    for i in range(requests_per_thread):
                        sleep(random())
                        data = b"x" * randint(1, 262144) + b"\n"
                        try:
                            s.sendall(data)
                            resp = r.readline()
                        except:
                            s = connect_to(ifc)
                            r = s.makefile("rb")
                            continue
                        else:
                            assert resp == b""

                ths = [ Thread(target = th_proc) for i in range(max_connections) ]
                for th in ths: th.start()
                for th in ths: th.join()

                sleep(request_timeout) # this makes all the queued requests expire

                # and the next request is processed as though nothing happened

                start = time()

                s = connect_to(ifc)
                r = s.makefile("rb")
                data = b"ok\n"
                s.sendall(data)
                assert r.readline() == data

                assert time() - start < 1.0

            finally:
                ifc.cease(); ifc.stop()

        test_lockout()

        ###############################

        pmnc.log.message("****************** DOS TEST ******************")

        class SlowLineEchoParser(LineEchoParser):
            def process_tcp_request(self):
                sleep(request_timeout * random() / 3)
                LineEchoParser.process_tcp_request(self)

        def test_dos():
            ifc = start_interface(SlowLineEchoParser, max_connections = 50)
            try:

                start = time()

                def th_proc():
                    while time() < start + 60.0:
                        data = b"x\n"
                        try:
                            s = connect_to(ifc)
                            s.sendall(data)
                            s.select([s], [], [], 1.0)
                        except:
                            pass

                ths = [ Thread(target = th_proc) for i in range(50) ]
                for th in ths: th.start()
                for th in ths: th.join()

                sleep(request_timeout) # this makes all the queued requests expire

                # and the next request is processed as though nothing happened

                start = time()

                s = connect_to(ifc)
                r = s.makefile("rb")
                data = b"x\n"
                s.sendall(data)
                assert r.readline() == data

                assert time() - start < request_timeout / 3.0 + 1.0

            finally:
                ifc.cease(); ifc.stop()

        test_dos()

    ###################################

    test_interface(None, None) # tcp
    test_interface(os_path.join(__cage_dir__, "ssl_keys", "key_cert.pem"),
                   os_path.join(__cage_dir__, "ssl_keys", "ca_cert.pem")) # ssl

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF