"""Utility functions for coroutines.
THIS FILE SHOULD HAVE NO NON-STANDARD-LIBRARY DEPENDENCIES.
"""


class AsyncPass:
    """Equivalent of the pass statement for async functions.
    This is really useful if you want to have a non-async function which
    returns a coroutine, but has some cases where it wants to do nothing.

    For instance you could do
    >>> async def maybe_log(i: int):
    ...     if i > 0:
    ...         await log(i=i)
    ... await maybe_log(10)
    ... await maybe_log(-10)

    but if you wanted to instead implement the same thing eagerly it would
    look like
    >>> def maybe_log(i: int):
    ...     if i > 0:
    ...         return log(i=i)
    ...     return async_pass
    ... await maybe_log(10)
    ... await maybe_log(-10)

    Most of the time you should just write the async version, but there's
    cases, eg. when implementing python magic methods or other interfaces
    that aren't allowed to be async.
    """

    def __await__(self):
        if False:
            yield


async_pass = AsyncPass()


class AsyncYield:
    def __await__(self):
        return (yield None)


async_yield = AsyncYield()
