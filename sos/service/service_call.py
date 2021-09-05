import dataclasses
import functools
import inspect

from ..execution_context import ExecutionContext, current_execution_context
from typing import Awaitable, Callable, Generic, Optional, Tuple, TypeVar

# TODO
#   - better ServiceCall reprs -- this is the sort of thing that makes interfaces nicer / more
#       discoverable, so really can't get too fancy, go ham here


T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


class ServiceCallBase(Awaitable[T]):

    _completed: bool = False
    _result: T = None
    _exception: Exception = None

    def __await__(self) -> T:
        """This is the key of how this whole system works. `await`ing on a ServiceCall
        instance yields the coroutine down into `kernel_main`, which can then validate
        and execute the call. In normal asyncio this would be illegial; in the asyncio
        event loops yielding a non-Task value throws a RuntimeException. This is
        implementation specific to the asyncio library; rather async and await methods
        are just coroutines and yielding values is fine! Since we can orchestrate the
        coroutine entirely on our own from Kernel.main, the underlying event loop never
        sees the yielded value, and we're free to use async/await syntax for coroutines.

        When Kernel.main does `send`, yield returns a value, ie. the service call result;
        that's what we return here, and then awaiting on the ServiceCall gives the
        return value from the actual call.
        """
        if not self._completed:
            try:
                self._result = yield self
            except Exception as e:
                self._exception = e
            finally:
                self._completed = True
        if self._exception is not None:
            raise self._exception
        return self._result


async def schedule(coro: Awaitable[T]) -> ServiceCallBase[T]:
    """Schedule a coroutine or service call to be put into the task pool.
    It will run in "parallel", and can either be awaited on or not. Even
    if it is never awaited, it will eventually complete."""
    token = await Schedule(current_execution_context(), coro)
    return AwaitScheduled(token)


async def gather(*awaitables: Awaitable[any]) -> Tuple[any, ...]:
    """Await on any number of awaitable coroutines or service calls. They
    can be executed in parallel before re-scheduling this task."""
    all_scheduled = [await schedule(awaitable) for awaitable in awaitables]
    return tuple([await scheduled for scheduled in all_scheduled])


def as_awaitable(fn):
    @functools.wraps(fn)
    async def awaitable(*args, **kwargs):
        result = fn(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    return awaitable


@dataclasses.dataclass
class ServiceCall(ServiceCallBase):
    """ServiceCall is the Awaitable / promise type which Service client stub methods
    return. They act like promises moreso than Awaitables, in that you can await on them
    as many times as you want, and they'll retain a reference to their result value until
    they're garbage collected.

    The ServiceCall serves as a data class to pass details of a service call request
    down to the kernel.
    Simply we're expecting the kernel to execute the following:

    >>> service = lookup_or_start_service(service)
    >>> endpoint = getattr(service, endpoint)
    >>> with execution_context.active():
    ...    result = endpoint(*args, **kwargs)
    ...    return await transform_result(result)

    ServiceCall also supports a `transform_result` parameter, along with some helpers,
    which allow it to specify a computation graph that will be passed to the kernel.
    Importantly this is scheduled by the kernel before returning to the awaiting coroutine,
    _or if the backend service is remote, it is scheduled by the remote kernel_. That means
    we can implement TIME TRAVEL, aka we can send a chain of multiple calls in a dependency
    graph to a remate service, _while only paying the round-trip latency cost once_.

    For instance
    >>> await Services().list_services().flat_map(Services().list_backends)
    will execute a dynamic number of service calls without having to round-trip for each!

    This comes at a cost of some mental complexity in that it might not be obvious when
    reading code what's expected to be executed locally vs potentially remotely. For instance,

    >>> await Services().list_services.flat_map(Services('highlander-2').list_backends)

    if Services() resolves to a remote service backend, then Services('highlander-2') will
    be resolved remotely rather than locally; similarly eg. any internal log calls will log
    remotely.

    The best way to think about this complexity is to realize that awaiting a ServiceCall
    is an atomic unit of computation which will be computed entirely in one context.

    There's also a number of helpful methods for building up the computation graph in
    transform_result:
        - .apply()
        - .map()
        - .flat_map()
        - .filter()
        - .or_else()
    All of these functions take a function which can be an async or synchronous python
    function, and will return a new ServiceCall with a transform_result function set
    with the appropriate transformation.
    """

    # Desired execution context for the system call to run in
    execution_context: ExecutionContext
    # The type of the service to call; the kernel is responsible for mapping this
    service: type["service.Service"]
    # If there are multiple implementations of the service running and available to
    # the user, then IDs can differentiate them. If you don't care which one you get,
    # you don't have to specify.
    service_id: Optional[str]
    # The name of the endpoint method to call on the service
    endpoint: str
    # args and kwargs for the endpoint method.
    args: list[any]
    kwargs: dict[str, any]
    # The mechanism by which Time Travel is implemented. ServiceCalls can specify
    # an arbitrary coroutine function which will be run on the result, which can
    # include making more service calls. Locally, this is scheduled for execution
    # by the kernel. In the remote case this computation happens entirely on the
    # remote kernel.
    transform_result: Optional[Callable[[T], Awaitable[U]]] = None

    def apply(self, transform_result: Callable[[T], Awaitable[U]]) -> "ServiceCall[U]":
        if self.transform_result:
            transform_result = compose_coros(transform_result, self.transform_result)
        return dataclasses.replace(self, transform_result=transform_result)

    def map(
        self: "ServiceCall[list[T]]", fn: Callable[[T], Awaitable[U]]
    ) -> "ServiceCall[list[U]]":
        fn = as_awaitable(fn)

        async def _map(l):
            return await gather(*(fn(v) for v in l))

        return self.apply(_map)

    def flat_map(
        self: "ServiceCall[list[T]]", fn: Callable[[T], Awaitable[list[U]]]
    ) -> "ServiceCall[list[U]]":
        fn = as_awaitable(fn)

        async def _flat_map(l):
            ll = await gather(*[fn(t) for t in l])
            return [u for l in ll for u in l]

        return self.apply(_flat_map)

    def filter(
        self: "ServiceCall[list[T]]", fn: Callable[[T], Awaitable[U]]
    ) -> "ServiceCall[list[U]]":
        fn = as_awaitable(fn)

        async def _filter(l):
            preds = await gather(*[fn(t) for t in l])
            return [t for t, p in zip(l, preds) if p]

        return self.apply(_filter)

    def or_else(
        self: "ServiceCall[T]", fn: Callable[[], Awaitable[U]]
    ) -> "ServiceCall[U]":
        fn = as_awaitable(fn)

        async def _or_else(t):
            if t:
                return t
            return await fn()

        return self.apply(_or_else)

    def __getattr__(self, attr: str):
        """I don't like this, I'm pretty likely to remove it before it goes to far :P"""
        if attr.startswith("__"):
            return super().__getattribute__(attr)
        return self.apply(as_awaitable(lambda v: getattr(v, attr)))

    def __hash__(self):
        # necessary for storing weakrefs
        return id(self)


def compose_coros(
    f: Callable[[T], Awaitable[U]], g: Callable[[U], Awaitable[V]]
) -> Callable[[T], V]:
    """Compose two unary async functions into one unary async function that awaits
    on each in sequence. The async equivalent of composed = lambda x: f(g(x))."""

    async def composed(t: T) -> V:
        return await f(await g(t))

    return composed


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
