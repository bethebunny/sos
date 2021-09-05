import contextlib
import contextvars
import dataclasses
import functools
import inspect
from typing import Awaitable, Callable, Optional, TypeVar

from ..execution_context import current_execution_context
from .service_call import ServiceCall


# TODO:
#   - Errors should be nice, eg. "did you mean...?"
#   - clean up Service documentation
#   - interface and type versioning
#   - interface and type serialization
#   - make backend __asyncinit__ log
#   - clean up current_call API

T = TypeVar("T")


_current_call = contextvars.ContextVar("current_call")


@contextlib.contextmanager
def current_call(backend_instance, endpoint_name):
    token = _current_call.set((backend_instance, endpoint_name))
    try:
        yield
    finally:
        _current_call.reset(token)


current_call.get = _current_call.get


class Error(Exception):
    """Base service call error."""


class ServiceNotFound(Error):
    """Didn't find the service in the services lookup."""


class ServiceDidNotStart(Error):
    """There was a failure starting a backend for the service."""


class ServiceHadNoMatchingEndpoint(Error):
    """The service backend for that service didn't have the requested method endpoint."""


def create_service_endpoint(
    service: type["Service"],
    endpoint_handle: Callable[..., Awaitable[T]],
) -> Callable[..., ServiceCall[T]]:
    """Replaces an async method on a service interface with one which actually executes
    the service call."""
    # Do as much work outside of wrapper as we can since this will be critical path
    # for every service call.
    return_annotation = inspect.signature(endpoint_handle).return_annotation
    return_type = (
        any if return_annotation is inspect.Signature.empty else return_annotation
    )

    @functools.wraps(endpoint_handle)
    def service_endpoint(self, *args, **kwargs) -> ServiceCall[return_type]:
        return ServiceCall[return_type](
            current_execution_context(),
            service,
            self.service_id,
            endpoint_handle.__name__,
            args,
            kwargs,
        )

    return service_endpoint


class ServiceMeta(type):
    SERVICES = []

    def __new__(cls, name, bases, namespace):
        # This could probably be implemented with __init_subclass__
        # but it's fine as it is for now :)

        client_type: type[Service] = super().__new__(cls, name, bases, namespace)

        # vars().update is gone in python3 / mappingproxy
        endpoints = [
            attr
            for attr, value in namespace.items()
            if inspect.iscoroutinefunction(value)
            and not getattr(value, "__is_client_method__", False)
        ]
        for attr in endpoints:
            service_endpoint = create_service_endpoint(client_type, namespace[attr])
            setattr(client_type, attr, service_endpoint)

        backend_bases = (
            *(base.Backend for base in bases if hasattr(bases, "Backend")),
            ServiceBackendBase,
        )

        backend_type = type(f"{name}.Backend", backend_bases, {})

        backend_type.interface = client_type
        client_type.Backend = backend_type
        client_type.__endpoints__ = endpoints

        cls.SERVICES.append(client_type)

        return client_type


class ServiceBackendBase:
    # can't actually inherit from Service.Backend because Service.Backend inherits from us :)

    @dataclasses.dataclass
    class Args:
        def __repr__(self):
            # Hack, Rich doesn't render dataclasses with no attributes properly
            return f"{type(self).__name__}(None)"

    def __init__(self, args: Optional[Args] = None):
        self.args = args or self.Args()

    async def __asyncinit__(self):
        """An async method called after init on all registered backend instances.
        Allows service backend initialization to make service calls.
        """

    async def shutdown(self):
        """Called by the kernel to shut down services. Override with any cleanup
        your service needs to do."""

    async def health_check(self) -> bool:
        return True

    async def endpoints(self) -> dict[str, inspect.Signature]:
        """Return"""  # TODO
        return {
            endpoint: inspect.signature(getattr(self, endpoint))
            for endpoint in self.interface.__endpoints__
        }

    async def __call__(self, service_call: ServiceCall):
        endpoint = getattr(self, service_call.endpoint)
        return await endpoint(*service_call.args, **service_call.kwargs)


def clientmethod(method):
    """Decorator to mark an async method on a Service as being client-side code. Rather
    than be stubbed out to await a ServiceCall, it will retain its real code and definition."""
    method.__is_client_method__ = True
    return method


class Service(metaclass=ServiceMeta):
    # The Service definition class is both an interface/client and the implementation.
    # When you define a Service class, the metaclass creates the separate
    # implementations, and this becomes a thin client interface. When yielding
    # out to the service calls, the kernel code will be able to look at
    # the ExecutionContext for the yielding task and pass it to the "backend"
    # implementation. This backend is the one that's actually able to run
    # "priveleged" code.
    """Service is the core class for implementing OS behaviors, and pretty much anything else.

    Subclasses of Service should (for now) have a zero-arg constructor that sets up an instance
    of the class. TODO: stubs should not do the same initialization the backend does >.>

    A Service implementation will have some async methods on it. Service subclasses handle async
    methods specially, and calling those methods will return a `ServiceResult[T] <: Awaitable[T]`
    rather than an `Awaitable[T]`.

    When you create a Service subclass, the resulting type will be a "service stub" or a client.
    What this means is that any methods which are `async` are replaced with a "stub" version of
    that method, which when it is called and awaited will yield a SystemCall object.

    The ServiceMeta.SERVICES registry (for now, this will deeeeefinitely change, hopefully soon)
    _also_ keeps a record of the original class type as defined. This is the "backend" service
    implementation which is constructed to actually run system calls.

    When the kernel executes a SystemCall, it will call the method on the backend instance with
    the args provided by the SystemCall object.


    Other thoughts for the continuing development of services and related tooling:

    it should be possible to define "service decorators", and your system should primarily be configured by
        1. determining which services are present and
        2. determining how those services are implemented

    Hmmm is that actually what I want? For SMC we just said "look at `address` for a service of service type
    `ServiceType`, I promise it's there"
    I think I kinda like the "capabilities" style better?
    What if you want multiple implementations of the same service?

    How does state management work? Certainly "client"s should not have substantial state; however
    it seems likely that the backend Service instances might have important state.

    Should we expect that connections are sticky? Expect that they're transient and that services
    should be ~stateless even if they have important state/setup?

    I think the best option is to not make any too strong of assumptions about this yet, but go forward
    with a guiding principle that practicality wins except in cases where we can make fundamentally more
    powerful tools through a stronger assumption that might make things less practical.
    """

    def __init__(self, service_id: Optional[str] = None):
        self.service_id = service_id

    async def health_check(self) -> bool:
        """Basic service instance health check."""

    async def endpoints(self) -> dict[str, inspect.Signature]:
        """Service endpoint discovery."""
