import typing
from typing import Awaitable, Callable, Generic

T = typing.TypeVar("T")


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
                # TODO: figure out if I definitely need this, it feels like
                #       this is better handled as logic in `execute_service_call`
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
