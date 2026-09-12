import pycares  # type: ignore
import socket
import weakref

from tornado.concurrent import Future
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.netutil import Resolver, is_valid_ip

import typing

if typing.TYPE_CHECKING:
    from typing import Generator, Any, List, Tuple, Dict  # noqa: F401


class CaresResolver(Resolver):
    """Name resolver based on the c-ares library.

    This is a non-blocking and non-threaded resolver.  It may not produce the
    same results as the system resolver, but can be used for non-blocking
    resolution when threads cannot be used.

    ``pycares`` will not return a mix of ``AF_INET`` and ``AF_INET6`` when
    ``family`` is ``AF_UNSPEC``, so it is only recommended for use in
    ``AF_INET`` (i.e. IPv4).  This is the default for
    ``tornado.simple_httpclient``, but other libraries may default to
    ``AF_UNSPEC``.

    .. versionchanged:: 5.0
       The ``io_loop`` argument (deprecated since version 4.1) has been removed.

    .. deprecated:: 6.2
       This class is deprecated and will be removed in Tornado 7.0. Use the default
       thread-based resolver instead.
    """

    def initialize(self) -> None:
        self.io_loop = IOLoop.current()
        self.fds = {}  # type: Dict[int, int]
        # Use a weakref to avoid circular reference through the callback
        weak_self = weakref.ref(self)
        def sock_state_cb(fd: int, readable: bool, writable: bool) -> None:
            self = weak_self()
            if self is None:
                return
            self._sock_state_cb(fd, readable, writable)
        self.channel = pycares.Channel(sock_state_cb=sock_state_cb)

    def _sock_state_cb(self, fd: int, readable: bool, writable: bool) -> None:
        state = (IOLoop.READ if readable else 0) | (IOLoop.WRITE if writable else 0)
        if not state:
            self.io_loop.remove_handler(fd)
            del self.fds[fd]
        elif fd in self.fds:
            self.io_loop.update_handler(fd, state)
            self.fds[fd] = state
        else:
            self.io_loop.add_handler(fd, self._handle_events, state)
            self.fds[fd] = state

    def _handle_events(self, fd: int, events: int) -> None:
        read_fd = pycares.ARES_SOCKET_BAD
        write_fd = pycares.ARES_SOCKET_BAD
        if events & IOLoop.READ:
            read_fd = fd
        if events & IOLoop.WRITE:
            write_fd = fd
        self.channel.process_fd(read_fd, write_fd)

    @gen.coroutine
    def resolve(
        self, host: str, port: int, family: int = 0
    ) -> "Generator[Any, Any, List[Tuple[int, Any]]]":
        if is_valid_ip(host):
            addresses = [host]
        else:
            fut = Future()  # type: Future[Tuple[Any, Any]]
            self.channel.getaddrinfo(
                host, port, family=family,
                callback=lambda result, error: fut.set_result((result, error))
            )
            result, error = yield fut
            if error:
                raise OSError(
                    "C-Ares returned error %s: %s while resolving %s"
                    % (error, pycares.errno.strerror(error), host)
                )
            addrinfo = []
            for node in result.nodes:
                if family != socket.AF_UNSPEC and family != node.family:
                    raise OSError(
                        "Requested socket family %d but got %d" % (family, node.family)
                    )
                # node.addr is a tuple like (b'127.0.0.1', 80) for IPv4
                # or (b'::1', 80, 0, 0) for IPv6
                address = node.addr[0]
                if isinstance(address, bytes):
                    address = address.decode()
                addrinfo.append((node.family, (address, port)))
            return addrinfo
        addrinfo = []
        for address in addresses:
            if "." in address:
                address_family = socket.AF_INET
            elif ":" in address:
                address_family = socket.AF_INET6
            else:
                address_family = socket.AF_UNSPEC
            if family != socket.AF_UNSPEC and family != address_family:
                raise OSError(
                    "Requested socket family %d but got %d" % (family, address_family)
                )
            addrinfo.append((typing.cast(int, address_family), (address, port)))
        return addrinfo
