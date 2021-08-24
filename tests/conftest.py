import asyncio
import functools
import os
import inspect
import pytest

from sos.kernel_main import kernel_main

# Set up hooks for vscode debugger integration
# see https://stackoverflow.com/questions/62419998/how-can-i-get-pytest-to-not-catch-exceptions/62563106#62563106
if os.getenv("_PYTEST_RAISE", "0") != "0":

    @pytest.hookimpl(tryfirst=True)
    def pytest_exception_interact(call):
        raise call.excinfo.value

    @pytest.hookimpl(tryfirst=True)
    def pytest_internalerror(excinfo):
        raise excinfo.value


# Much of the following was adapted from pytest-asyncio source


def is_async(a):
    return inspect.isasyncgen(a) or inspect.iscoroutine(a)


def pytest_configure(config):
    """Inject documentation."""
    config.addinivalue_line("markers", "kernel: an async test to be run in kernel_main")


@pytest.mark.tryfirst
def pytest_pycollect_makeitem(collector, name, obj):
    """A pytest hook to collect asyncio coroutines."""
    if collector.funcnamefilter(name) and is_async(obj):
        item = pytest.Function.from_parent(collector, name=name)

        # Due to how pytest test collection works, module-level pytestmarks
        # are applied after the collection step. Since this is the collection
        # step, we look ourselves.
        item = pytest.Function.from_parent(collector, name=name)  # To reload keywords.

        if "kernel" in item.keywords:
            return list(collector._genfunctions(name, obj))


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_pyfunc_call(pyfuncitem):
    """
    Run asyncio marked test functions in an event loop instead of a normal
    function call.
    """
    if "kernel" in pyfuncitem.keywords:
        pyfuncitem.obj = wrap_kernel_test(pyfuncitem.obj)
    yield


def wrap_kernel_test(func):
    """Return a sync wrapper around an async function executing it in the
    current event loop."""

    def dict_filter(d, p):
        return {k: v for k, v in d.items() if p(v)}

    @functools.wraps(func)
    def inner(**kwargs):
        loop = asyncio.get_event_loop()

        async def run_fixtures_and_test():
            normal_kwargs = dict_filter(kwargs, lambda v: not is_async(v))
            async_fixture_kwargs = dict_filter(kwargs, inspect.iscoroutine)

            async_gen_fixture_kwargs = {}
            async_gen_finalizers = {}

            for k, v in kwargs.items():
                if inspect.isasyncgen(v):
                    async_gen_fixture_kwargs[k] = await v.__anext__()
                    async_gen_finalizers[k] = v.__anext__()

            try:
                await func(
                    **{
                        **normal_kwargs,
                        **async_fixture_kwargs,
                        **async_gen_fixture_kwargs,
                    }
                )
            finally:
                for finalizer in async_gen_finalizers.values():
                    try:
                        await finalizer
                    except StopAsyncIteration:
                        pass  # expected
                    except Exception as e:
                        raise
                    else:
                        raise TypeError("async gen fixture {func} had multiple yields")

        coro = kernel_main(run_fixtures_and_test())
        task = asyncio.ensure_future(coro, loop=loop)
        try:
            loop.run_until_complete(task)
        except BaseException:
            # run_until_complete doesn't get the result from exceptions
            # that are not subclasses of `Exception`. Consume all
            # exceptions to prevent asyncio's warning from logging.
            if task.done() and not task.cancelled():
                task.exception()
            raise

    return inner
