import functools
import os
import inspect
import pytest

from sos.kernel_main import (
    Kernel,
    Scheduler,
    Services,
    TheServicesBackend,
    current_execution_context,
)
import sos.scheduler

sos.scheduler.RAISE_UNAWAITED_SCHEDULED_EXCEPTIONS = True

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


def is_async_function(a):
    return inspect.isasyncgenfunction(a) or inspect.iscoroutinefunction(a)


def pytest_configure(config):
    """Inject documentation."""
    config.addinivalue_line("markers", "kernel: an async test to be run in kernel_main")


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_pyfunc_call(pyfuncitem):
    """
    Run asyncio marked test functions in an event loop instead of a normal
    function call.
    """
    if "kernel" in pyfuncitem.keywords:
        kernel = pyfuncitem.funcargs["kernel"]
        test_func = pyfuncitem.obj
        pyfuncitem.obj = lambda **kwargs: kernel.main(test_func(**kwargs))
    yield


def pytest_runtest_setup(item):
    # Every kernel test needs a kernel instance to run in
    if "kernel" in item.keywords and "kernel" not in item.fixturenames:
        item.fixturenames.append("kernel")


# def wrap_kernel_test(func, kernel):
#     """Return a sync wrapper around an async function executing it in the
#     current event loop."""

#     def dict_filter(d, p):
#         return {k: v for k, v in d.items() if p(v)}

#     @functools.wraps(func)
#     def wrapper(**kwargs):
#         kernel.
#         async def run_test():
#             normal_kwargs = dict_filter(kwargs, lambda v: not is_async(v))
#             async_fixture_kwargs = dict_filter(kwargs, inspect.iscoroutine)

#             async_gen_fixture_kwargs = {}
#             async_gen_finalizers = {}

#             for k, v in kwargs.items():
#                 if inspect.isasyncgen(v):
#                     async_gen_fixture_kwargs[k] = await v.__anext__()
#                     async_gen_finalizers[k] = v.__anext__()

#             try:
#                 await func(
#                     **{
#                         **normal_kwargs,
#                         **async_fixture_kwargs,
#                         **async_gen_fixture_kwargs,
#                     }
#                 )
#             finally:
#                 for finalizer in async_gen_finalizers.values():
#                     try:
#                         await finalizer
#                     except StopAsyncIteration:
#                         pass  # expected
#                     except Exception:
#                         raise
#                     else:
#                         raise TypeError("async gen fixture {func} had multiple yields")

#         kernel_main(run_fixtures_and_test())

#     return inner


async def wrap_anext(anext):
    """Internally we store a weakref to the coroutine, which doesn't work for __anext__
    for python's builtin async generators."""
    return await anext


@pytest.hookimpl(hookwrapper=True)
def pytest_fixture_setup(fixturedef, request):
    """Adjust the event loop policy when an event loop is produced."""

    wants_kernel = "kernel" in fixturedef.argnames

    # We're replacing fixturedef.func, so we need to keep a durable
    # reference to it for our wrappers
    fixture_func = fixturedef.func

    if inspect.isasyncgenfunction(fixturedef.func):
        if not wants_kernel:
            fixturedef.argnames += ("kernel",)

        @functools.wraps(fixturedef.func)
        def wrapper(*args, **kwargs):
            kernel = kwargs["kernel"]
            if not wants_kernel:
                kwargs.pop("kernel")

            agen = fixture_func(*args, **kwargs)

            yield kernel.main(wrap_anext(agen.__anext__()))

            try:
                kernel.main(wrap_anext(agen.__anext__()))
            except StopAsyncIteration:
                pass
            else:
                raise ValueError(
                    f"Async generator fixture {fixturedef.argname} had multiple yields"
                )

        fixturedef.func = wrapper
    elif inspect.iscoroutinefunction(fixturedef.func):
        if not wants_kernel:
            fixturedef.argnames += ("kernel",)

        @functools.wraps(fixturedef.func)
        def wrapper(*args, **kwargs):
            kernel = kwargs["kernel"]
            if not wants_kernel:
                kwargs.pop("kernel")
            return kernel.main(fixture_func(*args, **kwargs))

        fixturedef.func = wrapper
    yield


@pytest.fixture
def kernel():
    services = TheServicesBackend()
    services.register_backend_instance(Services, services, "highlander")
    return Kernel(
        services=services,
        root_ec=current_execution_context(),
        scheduler=Scheduler(),
    )
