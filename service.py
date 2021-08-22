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

# WARNING: THING ABOUT THIS A LOT SOMETIME
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
    def __init__(self, handle: Awaitable[T]):
        super().__init__()
        self._handle = handle

    async def compute_result(self) -> T:
        # Override in subclasses
        return await self._handle


P = typing.TypeVar("P")


class DerivedServiceResult(ServiceResultBase[T], Generic[P, T]):
    async def compute_result(self) -> T:
        return await self.compute_result_from_parent(await self.parent)

    async def compute_result_from_parent(self, parent: P) -> T:
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
    # if we return a ServiceResult, we want to resolve it to a real value
    while isinstance((result := await result), ServiceResultBase):
        pass
    return result


async def resolve_args(args, kwargs):
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
    # not sure what these types are yet
    service: any
    endpoint: any
    args: list[any]
    kwargs: dict[str, any]

    def __await__(self):
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
    """A Service is a class which defines a service interface.
    ??? I still need to decide about "interfaces" vs "implementation", for instance
    it should be possible to define "service decorators", and your system should primarily be configured by
        1. determining which services are present and
        2. determining how those services are implemented

    Hmmm is that actually what I want? For SMC we just said "look at `address` for a service of service type
    `ServiceType`, I promise it's there"
    I think I kinda like the "capabilities" style better?
    What if you want multiple implementations of the same service?


    ... IN ANY CASE

    >>> Real non-crazy docs here
    A Service implementation will have some async methods on it. Service subclasses handle async
    methods specially, and calling those methods will return a `ServiceResult[T] <: Awaitable[T]`
    rather than an `Awaitable[T]`.
    <<<

    How does state management work? Certainly "client"s should not have substantial state; however
    it seems likely that the backend Service instances might have important state.

    Should we expect that connections are sticky? Expect that they're transient and that services
    should be ~stateless even if they have important state/setup? BLAH SO MANY DECISIONS
    I think the best option is to not make any too strong of assumptions about this yet, but go forward
    with a guiding principle that practicality wins except in cases where we can make fundamentally more
    powerful tools through a stronger assumption that might make things less practical.
    """

    # Remove this for now; I think it's deletable but not 100% yet. If you see this comment that
    # means you can delete it all :)
    """
    _backend = None

    @classmethod
    def backend(cls) -> "Service":
        # For now assume single event loop so we don't need to worry about creating
        # and leaking multiple backends
        if cls._backend is None:
            cls._backend = cls.Backend()
        return cls._backend
    def __init__(self, execution_context: Optional[ExecutionContext] = None):
        # Default execution context is the current user in a chroot of the current working directory
        self._execution_context = (
            execution_context or current_execution_context().chroot()
        )
    """


async def kernel_main(main: Coroutine):
    root_ec = ExecutionContext(User("root"), Path("/"), Path("/"))
    # (the name for this function is almost certainly wrong right now)
    # okay, so the plan here is
    #   - "main" is some coroutine
    #   - "service calls" need to actually _yield_ something
    #   - this code manually orchestrates their execution in yielding
    #   - it maintains execution context and manages passing service calls to backends

    EXIT = object()

    # when send raises StopIteration we're done
    def send(coro, value, threw):
        try:
            if threw:
                return coro.throw(type(value), value)
            else:
                return coro.send(value)
        except StopIteration:
            return EXIT

    # kicking service calls (.send(None)) might be a way to start them without yielding? idk yet
    service_call_result = None
    # threw is annoying state management to decide whether to .send a value or to
    # .throw an exception; both continue the coroutine and we need to treat the returns
    # the same way.
    threw = False
    while (service_call := send(main, service_call_result, threw)) is not EXIT:
        threw = False
        # 1. get the right execution context
        # TODO: verify that requested ec is a subset of permissions of ec
        requested_ec = service_call.execution_context
        # TODO: finer grained errors; tracebacks are good, system should be developer friendly
        try:
            # 2. find or set up the backend service
            # TODO: currently just constructing a new backend instance every time
            backend = ServiceMeta.SERVICES[service_call.service]()
            endpoint = getattr(backend, service_call.endpoint)
            # 3. schedule and await on the service call
            # 4. save result to service_call_result
            with requested_ec.active():
                # args has a reference to the client instance
                service_call_result = await endpoint(
                    *service_call.args, **service_call.kwargs
                )
        except Exception as e:
            # Instead of `send`, the next loop needs to use `throw`.
            service_call_result = e
            threw = True
