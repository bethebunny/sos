import collections
import dataclasses
import functools
import inspect
import random
import re
import typing
from typing import Awaitable, Callable, Generic, Optional

T = typing.TypeVar("T")

from ..execution_context import ExecutionContext, current_execution_context


# TODO:
#   - tasks aren't actually scheduled until they're awaited on
#   - when an exception is thrown, un-awaited tasks leak (should be cancelled)
#   - if a call exits without awaiting on a task, that task should still complete
#       - but don't await on it; we _should_ be able to do things like say "log this later"
#       - probably better to make this explicit, ie. you have to mark anything you're not waiting on
#   - a lot of the stack is in the machinery around ServiceResult computation and resolution.
#       good target for making stack traces better / easier to read.


class ServiceResult(Awaitable[T]):
    """ServiceResult is the Awaitable / promise type which Service client stub methods
    return. They act like promises moreso than Awaitables, in that you can await on them
    as many times as you want, and they'll retain a reference to their result value until
    they're garbage collected.

    ServiceResults are also special in how they're handled by SystemCalls.
    There's some set of operations that can be done on them; currently
        1. attribute access (eg. `result.value`)
        2. item lookup (eg. `result["key"]` or `result[-3:]`)
        3. function chaining via .apply, eg. result.apply(lambda x: x + 1)

    The result of any of these operations will give another ServiceResult object
    (technically a ServiceResult object), which can also be Awaited on.

    Even more importantly though, _these values can be passed into other service calls_.
    So you don't have to wait on your other service calls to get back, you can start
    computing on the future results of those things and start sending off even more
    service calls to be scheduled! The services passed ServiceResult objects as arguments
    will know how to resolve them, so eg. in cases where there's a large lag time between
    services, you can batch an entire graph of service calls and computations to the
    service (or several) and ship them all off at once, only waiting on the round trip
    a single time!
    """

    def __init__(self):
        self._completed = False
        self._result = None
        self._exception = None

    def __await__(self) -> T:
        return self._compute_result().__await__()

    async def _compute_result(self) -> T:
        # We do all of this so we don't need to think about whether there's multiple
        # handles to the same Awaitable laying around; python async method coroutines
        # don't normally implement "promise" semantics, in other words you can't
        # await on the same Awaitable more than once. ServiceResult is more of a
        # Promise, ie. you can await on it as many times as you want and get the
        # same result.
        if not self._completed:
            try:
                # keep resolving until we resolve to a real value
                self._result = await resolve_result(self.compute_result())
            except Exception as e:
                self._exception = e
            finally:
                self._completed = True
        if self._exception is not None:
            raise self._exception
        return self._result

    async def compute_result(self):
        """Subclasses must call __init__ and implement this method."""
        raise NotImplemented

    def __getattr__(self, attr) -> "ServiceResultAttr":
        return ServiceResultAttr(self, attr)

    def __getitem__(self, item) -> "ServiceResultItem":
        return ServiceResultItem(self, item)

    def apply(self, fn: Callable[[T], any]) -> "ServiceResultApply":
        return ServiceResultApply(self, fn)

    def _repr_expression(self):
        return_type = self.__orig_class__.__args__[0]
        return f"{return_type.__module__}.{return_type.__qualname__}"

    def __repr__(self):
        # TODO: this is the type of thing that makes the interface way more
        # explorable, so go ham with making it fancy
        complete = (
            "success"
            if self._completed and not self._exception
            else "failed"
            if self._completed
            else "scheduled"
        )
        return f"ServiceResult[{self._repr_expression()}] ({complete})"


class ServiceResultImpl(ServiceResult, Awaitable[T]):
    """Direct Service client stub responses."""

    def __init__(self, handle: Awaitable[T]):
        super().__init__()
        self._handle = handle

    async def compute_result(self) -> T:
        return await self._handle


P = typing.TypeVar("P")


class DerivedServiceResult(ServiceResult[T], Generic[P, T]):
    """Base class for derived computations done on ServiceResults."""

    async def compute_result(self) -> T:
        return await self.compute_result_from_parent(await self.parent)

    async def compute_result_from_parent(self, parent: P) -> T:
        """More useful function for defining derived computations."""
        raise NotImplemented


# TODO: use protocols to better describe this (if possible)
class ServiceResultAttr(DerivedServiceResult[P, T]):
    def __init__(self, parent: ServiceResult[P], attr: str):
        super().__init__()
        self.parent = parent
        self.attr = attr

    async def compute_result_from_parent(self, parent_result: P) -> T:
        return getattr(parent_result, self.attr)

    def _repr_expression(self):
        return f"{self.parent._repr_expression()}.{self.attr}"


class ServiceResultItem(DerivedServiceResult[P, T]):
    def __init__(self, parent: ServiceResult[P], item: any):
        super().__init__()
        self.parent = parent
        self.item = item

    async def compute_result_from_parent(self, parent_result: P) -> T:
        return parent_result[self.item]

    def _repr_expression(self):
        return f"{self.parent._repr_expression()}[{self.item}]"


class ServiceResultApply(DerivedServiceResult[P, T]):
    def __init__(self, parent: ServiceResult[P], fn: Callable[[P], T]):
        super().__init__()
        self.parent = parent
        self.fn = fn

    async def compute_result_from_parent(self, parent_result: P) -> T:
        # what if self.fn is a service call? will this still work?
        return self.fn(parent_result)

    def _repr_expression(self):
        return f"{self.fn}({self.parent._repr_expression()})"


async def resolve_result(result):
    """Resolve a ServiceResult to a result value."""
    # if we return a ServiceResult, we want to resolve it to a real value
    while isinstance((result := await result), ServiceResult):
        pass
    return result


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


@dataclasses.dataclass
class ServiceCall:
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
        print(f"ServiceMeta: Creating service {name}")

        client_type = super().__new__(cls, name, bases, namespace)

        # vars().update is gone in python3 / mappingproxy
        for attr, value in namespace.items():
            if inspect.iscoroutinefunction(value):
                setattr(client_type, attr, wrap_service_call(client_type, value))

        # TODO: need to think more carefully about what bases should be here.
        #       It definitely shouldn't have Service, but maybe should have
        #       eg. Service.Backend or something?
        backend_base = type(f"{name}.Backend", (ServiceBackendBase,), namespace)

        backend_base.interface = client_type
        client_type.Backend = backend_base

        cls.SERVICES.append(client_type)

        return client_type


class ServiceBackendBase:
    @dataclasses.dataclass
    class Args:
        pass

    def __init__(self, args: Optional[Args] = None):
        self.args = args or self.Args()


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


def _load_words(path: str = "/usr/share/dict/words"):
    valid_word_re = re.compile(r"^[a-z]{4,7}$")
    with open(path) as words:
        for word in map(lambda s: s.strip(), words):
            if valid_word_re.match(word):
                yield word


_SEMANTIC_HASH_DICT = list(_load_words())


def _random_word():
    return random.choice(_SEMANTIC_HASH_DICT)


def semantic_hash(len=2):
    return "-".join(_random_word() for _ in range(len))


class ServiceService(Service):
    async def register_backend(
        self,
        service: type[Service],
        backend: type[Service.Backend],
        args: Optional[Service.Backend.Args] = None,
        service_id: Optional[str] = None,
    ) -> str:
        pass

    async def list_services(self) -> list[Service]:
        pass

    async def list_backends(
        self, service: type[Service]
    ) -> list[(str, type[Service.Backend], Service.Backend.Args)]:
        pass


# @highlander
# It probably makes sense to be able to replace this too :)
# BUT NOT FOR NOW!
class TheServiceServiceBackend(ServiceService.Backend):
    def __init__(self):
        super().__init__()
        # self._registered: dict[dict[str, Service.Backend]] = collections.defaultdict(dict)
        # for now there's no difference between "registered" and "running"
        self._running: dict[
            type[Service], dict[str, Service.Backend]
        ] = collections.defaultdict(dict)
        self._running[ServiceService]["highlander"] = self

    async def register_backend(
        self,
        service: type[Service],
        backend: type[Service.Backend],
        args: Optional[Service.Backend.Args] = None,
        service_id: Optional[str] = None,
    ) -> str:
        service_id = service_id if service_id is not None else semantic_hash(2)
        args = args if args is not None else backend.Args()

        impls = self._running[service]
        # TODO
        # if service_id in impls:
        #    raise
        impls[service_id] = backend(args)
        return service_id

    async def list_services(self) -> list[Service]:
        return list(ServiceMeta.SERVICES)

    async def list_backends(
        self, service: type[Service]
    ) -> list[(str, type[Service.Backend], Service.Backend.Args)]:
        return [
            (service_id, type(backend), backend.args)
            for service_id, backend in self._running.get(service, {}).items()
        ]

    # TODO: this needs to be privileged, this should only be executed by the kernel
    #       ... crazy idea, maybe there's a different service interface for it for
    #       users? or ACTUALLY since I'm not using the client in the kernel I can just
    #       not add it to the interface!!! genius
    #       however, the TODO here is now that the kernel doesn't validate system calls,
    #       so I can leak backends by constructing and yielding a SystemCall that runs
    #       a method that's not on the interface xD
    async def get_backend(
        self, service: type[Service], service_id: Optional[str] = None
    ) -> Service.Backend:
        impls = self._running.get(service, {})
        if service_id is not None:
            return impls[service_id]
        else:
            if not impls:
                # TODO error types
                raise RuntimeError(f"no running service found for {service}")
            return next(iter(impls.values()))
