import typing
from typing import Awaitable, Callable, Generic

T = typing.TypeVar("T")


# TODO
#   - reduce the call stack overhead of these functions
#   - figure out a way to be able to pass deferred computations down to the kernel
#       in a way that it will know the ServiceCall they plan to execute;
#       probably use ScheduleToken() also. Then the kernel can implement time-travel
#       implicitly for computation graphs including eg. gather as currently implemented
#       by just batching multiple awaiting requests to a remote service.
#   - better reprs -- this is the sort of thing that makes interfaces nicer / more
#       discoverable, so really can't get too fancy, go ham here
#   - use protocols or other to better describe types of derived ServiceResults.
#   - more judicious / well reasoned use of resolve_result; right now I think it's
#       just sort of applied everywhere it might be needed :)
#   - think about whether and how ServiceResultApply applies to async / non-async fns


class ServiceResult(Awaitable[T]):
    """ServiceResult is the Awaitable / promise type which Service client stub methods
    return. They act like promises moreso than Awaitables, in that you can await on them
    as many times as you want, and they'll retain a reference to their result value until
    they're garbage collected.

    ServiceResults are also special in how they're handled by await.
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

    A ServiceResult is a Promise rather than a Future, so it's fine to await on it
    multiple times.
    """

    def __init__(self):
        self._completed = False
        self._result = None
        self._exception = None

    def __await__(self) -> T:
        return self._compute_result().__await__()

    async def _compute_result(self) -> T:
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
        raise NotImplementedError

    def __getattr__(self, attr) -> "ServiceResultAttr":
        return ServiceResultAttr(self, attr)

    def __getitem__(self, item) -> "ServiceResultItem":
        return ServiceResultItem(self, item)

    def apply(self, fn: Callable[[T], any]) -> "ServiceResultApply":
        return ServiceResultApply(self, fn)

    def _repr_expression(self):
        return_type = self.__orig_class__.__args__[0]
        if return_type is None:
            return repr(None)
        return f"{return_type.__module__}.{return_type.__qualname__}"

    def __repr__(self):
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
        raise NotImplementedError


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
        return self.fn(parent_result)

    def _repr_expression(self):
        return f"{self.fn}({self.parent._repr_expression()})"


async def resolve_result(result):
    """Resolve a ServiceResult to a result value."""
    while isinstance((result := await result), ServiceResult):
        pass
    return result
