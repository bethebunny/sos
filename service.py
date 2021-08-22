import contextlib
import dataclasses
import functools
import inspect
from pathlib import Path
import typing
from typing import Awaitable, Callable, Coroutine, Generic, Optional

T = typing.TypeVar("T")


@dataclasses.dataclass
class User:
    name: str


ROOT = User("root")


# The user _should not_ be able to possibly use an execution context
# that's not a subset of the permissions of their own execution context.
# Obviously this implementation doesn't do that yet.
#
# For the intended api is eg.
# with current_execution_context().replace(user=User("stef"), root="/home/stef"):
#     files = Files()
@dataclasses.dataclass(frozen=True)
class ExecutionContext:
    user: User
    root: Path
    working_directory: Path

    def chroot(self, new_root: Optional[Path] = None):
        if new_root is None:
            new_root = self.working_directory
        if new_root.is_absolute():
            new_root = new_root.relative_to("/")
        return dataclasses.replace(
            self,
            root=self.root.joinpath(new_root),
            working_directory=Path("/"),
        )

    @contextlib.contextmanager
    def active(self):
        print(f"Activating {self}")
        global _EXECUTION_CONTEXT
        old_execution_context = _EXECUTION_CONTEXT
        _EXECUTION_CONTEXT = self
        try:
            yield
        finally:
            _EXECUTION_CONTEXT = old_execution_context


_EXECUTION_CONTEXT = ExecutionContext(ROOT, Path("/"), Path("/"))

# WARNING: THINK ABOUT THIS A LOT SOMETIME
# Potential for security holes here. For instance, if we can pass a callback
# to a service and get it to execute it, and that callback grabs execution context,
# we could leak or allow setting an execution context that's not ours.
def current_execution_context():
    return _EXECUTION_CONTEXT


# TODO:
#   - tasks aren't actually scheduled until they're awaited on
#   - when an exception is thrown, un-awaited tasks leak (should be cancelled)
#   - if a call exits without awaiting on a task, that task should still complete
#       - but don't await on it; we _should_ be able to do things like say "log this later"
#       - probably better to make this explicit, ie. you have to mark anything you're not waiting on


class ServiceResultBase(Awaitable[T]):
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
            print((type(self._exception), self._exception))
            raise self._exception
        return self._result

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


class ServiceResult(ServiceResultBase, Awaitable[T]):
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
    (technically a ServiceResultBase object), which can also be Awaited on.

    Even more importantly though, _these values can be passed into other service calls_.
    So you don't have to wait on your other service calls to get back, you can start
    computing on the future results of those things and start sending off even more
    service calls to be scheduled! The services passed ServiceResult objects as arguments
    will know how to resolve them, so eg. in cases where there's a large lag time between
    services, you can batch an entire graph of service calls and computations to the
    service (or several) and ship them all off at once, only waiting on the round trip
    a single time!
    """

    def __init__(self, handle: Awaitable[T]):
        super().__init__()
        self._handle = handle

    async def compute_result(self) -> T:
        # Override in subclasses
        return await self._handle


P = typing.TypeVar("P")


class DerivedServiceResult(ServiceResultBase[T], Generic[P, T]):
    """Base class for derived computations done on ServiceResults."""

    async def compute_result(self) -> T:
        return await self.compute_result_from_parent(await self.parent)

    async def compute_result_from_parent(self, parent: P) -> T:
        """More useful function for defining derived computations."""
        raise NotImplemented


# TODO: use protocols to better describe this (if possible)
class ServiceResultAttr(DerivedServiceResult[P, T]):
    def __init__(self, parent: ServiceResultBase[P], attr: str):
        super().__init__()
        self.parent = parent
        self.attr = attr

    async def compute_result_from_parent(self, parent_result: P) -> T:
        return getattr(parent_result, self.attr)

    def _repr_expression(self):
        return f"{self.parent._repr_expression()}.{self.attr}"


class ServiceResultItem(DerivedServiceResult[P, T]):
    def __init__(self, parent: ServiceResultBase[P], item: any):
        super().__init__()
        self.parent = parent
        self.item = item

    async def compute_result_from_parent(self, parent_result: P) -> T:
        return parent_result[self.item]

    def _repr_expression(self):
        return f"{self.parent._repr_expression()}[{self.item}]"


class ServiceResultApply(DerivedServiceResult[P, T]):
    def __init__(self, parent: ServiceResultBase[P], fn: Callable[[P], T]):
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
    while isinstance((result := await result), ServiceResultBase):
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
            (await resolve_result(arg)) if isinstance(arg, ServiceResultBase) else arg
            for arg in args
        ],
        {
            k: (await resolve_result(v)) if isinstance(v, ServiceResultBase) else v
            for k, v in kwargs.items()
        },
    )


@dataclasses.dataclass
class ServiceCall:
    execution_context: ExecutionContext
    service: str
    endpoint: str
    args: list[any]
    kwargs: dict[str, any]

    def __await__(self):
        # TODO: document why this is this way, it's very particular and I have a cat :)
        result = yield self
        return result


def service_call_stub(service, async_fn):
    async def stub(args, kwargs):
        # we want to grab the execution context right before we yield up to
        # the kernel. this isn't a security issue but a usability one: if we
        # do this at the wrong time it will lead to really hard to trace timing
        # bugs with wrong contexts, and they'll likely be wrong in scary ways, ie.
        # not system-insecure but specifically different than the user wanted.

        # also, remove args[0] which is client.self
        return await ServiceCall(
            current_execution_context(), service, async_fn.__name__, args[1:], kwargs
        )

    return stub


async def execute_service_call(stub, args, kwargs):
    args, kwargs = await resolve_args(args, kwargs)
    return await stub(args, kwargs)


def wrap_service_call(
    service: any,  # Something; this is an ID of what service we're calling
    async_fn: Callable[..., Awaitable[T]],
) -> Callable[..., ServiceResult[T]]:
    return_annotation = inspect.signature(async_fn).return_annotation
    return_type = (
        any if return_annotation is inspect.Signature.empty else return_annotation
    )

    stub = service_call_stub(service, async_fn)

    @functools.wraps(async_fn)
    def wrapper(*args, **kwargs) -> ServiceResult[return_type]:
        # TODO: this currently resolves args in thread, which undoes any benefit of time traveling
        handle = execute_service_call(stub, args, kwargs)
        return ServiceResult[return_type](handle)

    return wrapper


class ServiceMeta(type):
    SERVICES = {}

    def __new__(cls, name, bases, dict):
        print(name)
        # Register a backend service which just executes the service code as-is
        cls.SERVICES[name] = super().__new__(cls, name, bases, dict)

        # Return a client
        return super().__new__(
            cls,
            name,
            bases,
            {
                k: wrap_service_call(name, v) if inspect.iscoroutinefunction(v) else v
                for k, v in dict.items()
            },
        )


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


# TODO: Errors should be nice, eg. "did you mean...?"
class Error(Exception):
    """Base service call error."""


class ServiceNotFound(Error):
    """Didn't find the service in the services lookup."""


class ServiceDidNotStart(Error):
    """There was a failure starting a backend for the service."""


class ServiceHadNoMatchingEndpoint(Error):
    """The service backend for that service didn't have the requested method endpoint."""


class InvalidExecutionContextRequested(Error):
    """The execution context requested in the service call asked for permissions it
    doesn't have."""


def validate_execution_context(ec: ExecutionContext, requested_ec: ExecutionContext):
    """Do security checks; if the requested execution context requests more permissions
    than the current one, reject it and raise an exception."""
    # For now just checking that we're not breaking chroot.
    abs_root = ec.root.resolve()
    requested_root = requested_ec.root.resolve()
    if not requested_root.is_relative_to(abs_root):
        raise InvalidExecutionContextRequested(
            f"New root was not a subset of the old root: {requested_ec.root}"
        )


async def lookup_service_backend(service: str) -> Service:
    """Look up the requesteb backend service. If it's not already running, start it."""
    if service not in ServiceMeta.SERVICES:
        raise ServiceNotFound(service)
    backend_class = ServiceMeta.SERVICES[service]
    try:
        # TODO: currently just constructing a new backend instance every time
        return ServiceMeta.SERVICES[service]()
    except Exception as e:
        raise ServiceDidNotStart(service) from e


async def handle_service_call(service_call: ServiceCall) -> any:
    """
    1. validate and set the execution context
    2. find and/or set up the backend service
    3. schedule and await on the service call
    4. save result to service_call_result
    """
    requested_ec = service_call.execution_context
    validate_execution_context(current_execution_context(), requested_ec)
    backend = await lookup_service_backend(service_call.service)
    if not hasattr(backend, service_call.endpoint):
        raise ServiceHadNoMatchingEndpoint(service_call.service, service_call.endpoint)
    endpoint = getattr(backend, service_call.endpoint)
    # what if the endpoint itself is making service calls? I think that's next up :P
    with requested_ec.active():
        return await endpoint(*service_call.args, **service_call.kwargs)


async def kernel_main(main: Coroutine):
    """Orchestrates a program (main), allowing it to yield ServiceCall objects,
    which are then executed and the results sent back to the coroutine.
    Also manages execution context including validation and security checks,
    and manages looking up (and possibly starting) service backends.

    Make sure to use fine grained errors.
    Also, tracebacks are good! System should be developer friendly,
    err on the side of more info even if it exposes system internals.
    """
    # We need to decide whether to .send or .throw depending on whether there was an
    # error executing the service call. Sending exceptions via .throw allows programs
    # to handle service call exceptions normally and potentially recover.
    # On the first pass we send(None) to start the coroutine.
    service_call_result = None
    last_service_call_threw = False

    while True:
        # Execute the main program until it yields a ServiceCall object.
        # When send or throw raise StopIteration, the program has exited.
        try:
            if last_service_call_threw:
                service_call = main.throw(
                    type(service_call_result), service_call_result
                )
            else:
                service_call = main.send(service_call_result)
        except StopIteration:
            return
        last_service_call_threw = False

        # The program made a service call.
        try:
            service_call_result = await handle_service_call(service_call)
        except Exception as e:
            service_call_result = e
            last_service_call_threw = True
