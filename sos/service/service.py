import dataclasses
import functools
import inspect
from typing import Awaitable, Callable, Generic, Optional, Tuple, TypeVar

from .service_result import ServiceResult, ServiceResultImpl, resolve_result
from sos.execution_context import ExecutionContext, current_execution_context


# TODO:
#   - tasks aren't actually scheduled until they're awaited on
#   - when an exception is thrown, un-awaited tasks leak (should be cancelled)
#   - if a call exits without awaiting on a task, that task should still complete
#       - but don't await on it; we _should_ be able to do things like say "log this later"
#       - probably better to make this explicit, ie. you have to mark anything you're not waiting on
#   - a lot of the stack is in the machinery around ServiceResult computation and resolution.
#       good target for making stack traces better / easier to read.


async def resolve_args(args, kwargs):
    """Replace any ServiceResults in args and kwargs with resolved values.
    This lets us move from "maybe there's some promises mixed in" ServiceResult land
    to "all values are now real and we can do whatever normal computation we want".
    """
    return (
        # In theory this should be a `tuple`, but we're splatting it anyway,
        # and list comprehensions are supported via https://www.python.org/dev/peps/pep-0530/
        # https://bugs.python.org/issue32113 claims that list comprehensions and
        # generator comprehensions are fundamentally different in this way; I disagree,
        # and I haven't figured out how you'd actually make the tuple() example work
        # without allocating the list.
        [
            (await resolve_result(arg)) if isinstance(arg, ServiceResult) else arg
            for arg in args
        ],
        {
            k: (await resolve_result(v)) if isinstance(v, ServiceResult) else v
            for k, v in kwargs.items()
        },
    )


class ServiceCallBase:
    def __await__(self):
        # This is the key of how this whole system works. `await`ing on a ServiceCall
        # instance yields the coroutine down into `kernel_main`, which can then validate
        # and execute the call. In normal asyncio this would be illegial; in the normal
        # event loops, yielding a value is illegal and throws a RuntimeException. This
        # is implementation specific to the asyncio library; rather async and await
        # methods are just coroutines and yielding values is fine! Since we can
        # orchestrate the coroutine entirely on our own from kernel_main, the underlying
        # event loop never sees the yielded value, and we're free to use async/await
        # syntax for coroutines like we want :)
        #
        # when kernel_main does `send`, yield returns a value, ie. the service call result;
        # that's what we return here, and then awaiting on the ServiceCall gives the
        # return value from the actual call.
        #
        # It's possible that there should be a debug flag which shows the full stack trace
        # (current behavior) but by default remove the kernel aspects; they're already
        # muddying the stack quite a bit, but at least for now it's still informative.
        return (yield self)


@dataclasses.dataclass
class ServiceCall(ServiceCallBase):
    """Data class for a ServiceCall to be passed down to the kernel to execute.
    Simply we're the kernel to execute the following:

    service = lookup_or_start_service(service)
    endpoint = getattr(service, endpoint)
    with execution_context.active():
        return endpoint(*args, **kwargs)
    """

    # Desired execution context for the system call to run in
    execution_context: ExecutionContext
    # The type of the service to call; the kernel is responsible for mapping this
    service: type["Service"]
    # If there are multiple implementations of the service running and available to
    # the user, then IDs can differentiate them. If you don't care which one you get,
    # you don't have to specify.
    service_id: Optional[str]
    # The name of the endpoint method to call on the service
    endpoint: str
    # args and kwargs for the endpoint method
    args: list[any]
    kwargs: dict[str, any]


T = TypeVar("T")


# TODO: the kernel and ServiceResult deleter should coordinate to automatically
#       schedule calls which were created but never awaited (or maybe error instead).
# TODO: "schedule" also being an english noun for a time plan is awkward
@dataclasses.dataclass
class Schedule(ServiceCallBase, Generic[T]):
    """A ServiceCall that schedules a ServiceResult or coroutine. This adds
    the coroutine to the Scheduler queue as ready, but doesn't wait on it yet.
    You can await on it or not in your process later, and use normal ServiceResult
    semantics on it ie. deferred computation.
    """

    execution_context: ExecutionContext
    awaitable: Awaitable[T]


class ScheduleToken:
    """Special token used by the Scheduler to track scheduled results."""


@dataclasses.dataclass
class AwaitScheduled(ServiceCallBase):
    """Wait on a previously scheduled coroutine."""

    token: ScheduleToken


async def schedule(coro: Awaitable[T]) -> ServiceResult[T]:
    """Schedule a coroutine or service call to be put into the task pool.
    It will run in "parallel", and can either be awaited on or not. Even
    if it is never awaited, it will eventually complete."""
    if not isinstance(coro, ServiceResult):
        coro = ServiceResultImpl(coro)
    token = await Schedule(current_execution_context(), coro)
    return AwaitScheduled(token)


def gather(*calls: Awaitable[any]) -> ServiceResult[Tuple[any]]:
    """Await on any number of awaitable coroutines or service calls. They
    can be executed in parallel before re-scheduling this task."""
    return ServiceResultImpl[Tuple[any]](_gather(calls))


async def _gather(calls: Awaitable[any]) -> Tuple[any]:
    all_scheduled = [await schedule(call) for call in calls]
    return tuple([await scheduled for scheduled in all_scheduled])


async def execute_service_call(service, service_id, endpoint_handle, args, kwargs):
    """Do arg resolution, build and yield ServiceCall based on current execution context."""
    # We want to be very specific about when we grab the execution context
    # this isn't a security issue from a sytem standpoint since the kernel will
    # get to validate that the EC we're passing; however if it's done at the wrong
    # time, for instance if some callback is executed that switches the active EC
    # in subtle way, we'll introduce a really hard to grok permissions bug where
    # we're giving permissions the user doesn't expect.

    # In this case since we wait for all of these to fully resolve, they shouldn't
    # be able to leak a context where we changed EC, so we can grab current EC after.
    args, kwargs = await resolve_args(args, kwargs)

    return await ServiceCall(
        current_execution_context(),
        service,
        service_id,
        endpoint_handle.__name__,
        args,
        kwargs,
    )


def wrap_service_call(
    service: type["Service"],
    endpoint_handle: Callable[..., Awaitable[T]],
) -> Callable[..., ServiceResult[T]]:
    """Replaces an async method on a service interface with one which actually executes
    the service call."""
    # Do as much work outside of wrapper as we can since this will be critical path
    # for every service call.
    return_annotation = inspect.signature(endpoint_handle).return_annotation
    return_type = (
        any if return_annotation is inspect.Signature.empty else return_annotation
    )

    @functools.wraps(endpoint_handle)
    def wrapper(self, *args, **kwargs) -> ServiceResult[return_type]:
        handle = execute_service_call(
            service, self.service_id, endpoint_handle, args, kwargs
        )
        return ServiceResultImpl[return_type](handle)

    return wrapper


class ServiceMeta(type):
    SERVICES = []

    def __new__(cls, name, bases, namespace):
        # This could probably be implemented with __init_subclass__
        # but it's fine as it is for now :)

        client_type = super().__new__(cls, name, bases, namespace)

        # vars().update is gone in python3 / mappingproxy
        endpoints = [
            attr
            for attr, value in namespace.items()
            if inspect.iscoroutinefunction(value)
        ]
        for attr in endpoints:
            setattr(client_type, attr, wrap_service_call(client_type, namespace[attr]))

        # TODO: name not being saved properly here; it omits `.Backend` which is confusing
        # TODO: if a service inherits from another service, its Backend class should also
        #       inherit from that service's Backend class (for subclass checks etc)
        backend_base = type(f"{name}.Backend", (ServiceBackendBase,), namespace)

        backend_base.interface = client_type
        client_type.Backend = backend_base
        client_type.__endpoints__ = endpoints

        cls.SERVICES.append(client_type)

        return client_type


class ServiceBackendBase:
    # This can't actually inherit from Service.Backend because Service.Backend inherits
    # from this :)
    @dataclasses.dataclass
    class Args:
        pass

    def __init__(self, args: Optional[Args] = None):
        self.args = args or self.Args()

    async def health_check(self) -> bool:
        return True


class Service(metaclass=ServiceMeta):
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
