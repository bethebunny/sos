import asyncio
import dataclasses
import functools
from typing import Optional, Tuple, Type, TypeVar
import uuid

import dill

from sos.execution_context import ExecutionContext, current_execution_context
from . import Service, ServiceCall
from sos.services.authentication import Authentication

S = TypeVar("S", bound=Service)

# TODO
#   - real authentication
#   - shared session tokens across networks
#       or otherwise locally-verifiable session tokens from a central auth service
#   - session tokens should not be reused with a different execution context
#   - get time travel / batch queries working for remote services


@dataclasses.dataclass(frozen=True, unsafe_hash=True)
class RemoteToken:
    """Base class for tokens from other kernels."""

    token: uuid.uuid4 = dataclasses.field(default_factory=uuid.uuid4)


class RemoteScheduleToken(RemoteToken):
    """A token representing a ScheduleToken on a remote kernel."""


class LoginToken(RemoteToken):
    """Login information for authentication. Stub for now."""


class SessionToken(RemoteToken):
    """A session token which is returned by authentication for reuse."""


@dataclasses.dataclass
class Session:
    execution_context: ExecutionContext


class Error(Exception):
    """Base error class for remote service access."""


class AuthenticationFailed(Error):
    """Attempted Authentication.authenticate but failed."""


class NoActiveSession(Error):
    """Exception indicating that the user tried to make a non-authentication
    service call without providing a valid authenticated session token."""


RemoteSpec = Tuple[str, int]


class Remote:
    """A backend type factory that allows creating Remote implementations of services.

    For instance,

    >>> await Services().register_backend(Files, Remote[Files], Remote.Args((host, port)))
    >>> await Files().list_directory()

    will let you use a `Files` service running at `host`!

    Currently ServiceCalls are serialized with dill, and passed to a remote kernel running
    a RemoteHostBackend.

    There's currently no way to list available services on a remote without connecting to it,
    and time travel isn't implemented for remote calls; each remote call executes eagerly.
    """

    @dataclasses.dataclass
    class Args:
        remote_id: RemoteSpec
        remote_service_id: Optional[str] = None

    # TODO: this is insufficient because a session should only be valid for a given ExecutionContext
    # TODO: services on the same network should be able to use one session to communicate with
    #       any other service
    _sessions: dict[RemoteSpec, SessionToken] = {}

    async def __call_remote(self, service_call) -> any:
        session_token = self._sessions.get(self.args.remote_id)
        if not session_token and service_call.service is not Authentication:
            session_token = await self.create_session()
        reader, writer = await asyncio.open_connection(*self.args.remote_id)
        writer.write(dill.dumps((session_token, service_call)))
        writer.write_eof()
        try:
            response = dill.loads(await reader.read())
        except NoActiveSession:
            print("Session invalid, attempting to create new session")
            if await self.create_session():
                print("Retrying")
                return await self.__call_remote(service_call)
            else:
                raise RuntimeError("Got back empty session token from remote auth")
        else:
            if isinstance(response, Exception):
                raise response
            else:
                return response

    async def create_session(self):
        spec = self.args.remote_id
        remote = Remote[Authentication](self.Args(spec))
        token = self._sessions[spec] = await remote.authenticate(None)
        return token

    _class_cache: dict[Type[Service], Type["Remote"]] = {}

    @classmethod
    def __class_getitem__(cls, item):
        if not issubclass(item, Service):
            raise TypeError(f"Remote must take a Service type parameter; got {item}.")
        if not (cached := cls._class_cache.get(item)):
            cached = cls._class_cache[item] = cls.make_remote_backend(item)
        return cached

    @classmethod
    def make_remote_backend(cls, service_type: Type[Service]):
        def make_endpoint(endpoint):
            @functools.wraps(getattr(service_type, endpoint))
            async def remote_endpoint(self, *args, **kwargs):
                service_call = ServiceCall(
                    current_execution_context(),
                    service_type,
                    self.args.remote_service_id,
                    endpoint,
                    args,
                    kwargs,
                )
                return await self.__call_remote(service_call)

            return remote_endpoint

        endpoints = {
            endpoint: make_endpoint(endpoint) for endpoint in service_type.__endpoints__
        }

        return type(
            f"Remote[{service_type.__name__}].Backend",
            (cls, service_type.Backend),
            endpoints,
        )
