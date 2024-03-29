import asyncio
import dataclasses
from pathlib import Path
import threading

import pytest

from sos import service
from sos.execution_context import ExecutionContext, User, current_execution_context
from sos.kernel_main import ServiceHadNoMatchingEndpoint, kernel_main
from sos.service import Service, ServiceCall, ServiceNotFound
from sos.service.service_call import AwaitScheduled, ScheduleToken
from sos.services import Services
from sos.util.coro import async_yield


# TODO:
#  - move many of these tests to test_service.py or test_services.py
#  - figure out test_service_call_delayed_execution_doesnt_leak_execution_context


class CustomError(Exception):
    pass


class A(Service):
    async def inc(self, x: int) -> int:
        pass

    async def triangle(self, x: int) -> int:
        pass


class SimpleA(A.Backend):
    async def inc(self, x: int) -> int:
        return x + 1

    async def triangle(self, x: int) -> int:
        return (x * (x + 1)) // 2


class OutsourceA(A.Backend):
    @dataclasses.dataclass
    class Args:
        outsource_id: str

    async def inc(self, x: int) -> int:
        return await A(self.args.outsource_id).inc(x)

    async def triangle(self, x: int) -> int:
        if x > 0:
            return x + await A(self.args.outsource_id).triangle(x - 1)
        return x


@pytest.fixture
async def services():
    simple_a_id = await Services().register_backend(A, SimpleA)
    outsource_a_id = await Services().register_backend(
        A,
        OutsourceA,
        OutsourceA.Args(outsource_id=simple_a_id),
    )
    yield (simple_a_id, outsource_a_id)


def test_kernel_tests_execute(kernel):
    # Normally doesn't return anything; verify that an exception
    # will propogate up above the kernel
    async def main():
        await async_yield
        raise CustomError

    with pytest.raises(CustomError):
        kernel.main(main())


# TODO: hmm this is super broken :/
#       it's not even testing the original thing (which I still need to test)
#       but apparently calling .close() here is removing the future from asyncio loop's
#       queue, but not actually resolving the future.
@pytest.mark.skip
@pytest.mark.kernel
async def test_cancelled_error_doesnt_exit_kernel():
    wait_forever = asyncio.sleep(3)
    token = await service.schedule(wait_forever)
    wait_forever.close()
    with pytest.raises(asyncio.CancelledError):
        await token


@pytest.mark.skip
@pytest.mark.kernel
async def test_cancelled_error_doesnt_exit_kernel_2():
    wait_forever = asyncio.sleep(3)
    token = await service.schedule(wait_forever)
    wait_forever.close()
    with pytest.raises(asyncio.CancelledError):
        await token


@pytest.mark.kernel
async def test_cancelling_future_passes_exception_to_awaiting_task():
    # TODO: this might just be bad behavior that I'm encoding in unit tests now.
    #       I would love an exception like AwaitedOnCancelledFuture instead.
    # TODO: this test still takes 3 seconds to run even though the future is cancelled
    future = asyncio.create_task(asyncio.sleep(3))
    future.cancel()
    with pytest.raises(asyncio.InvalidStateError):
        await future


@pytest.mark.skip
def test_kernel_tests_catch_raised_scheduled_exceptions():
    # Verify that in the test environment, a scheduled task throwing an exception will raise
    # This currently causes weird interactions with the test environment where
    # the deleter gets called during garbage collection at random points during other tests :P

    async def scheduled():
        await async_yield
        raise CustomError

    async def main():
        await service.schedule(scheduled())

    with pytest.raises(CustomError):
        kernel_main(main())


def test_kernel_outside_tests_dont_raise_scheduled_exceptions():
    import sos.scheduler

    try:
        sos.scheduler.RAISE_UNAWAITED_SCHEDULED_EXCEPTIONS = False

        async def scheduled():
            await async_yield
            raise CustomError

        async def main():
            await service.schedule(scheduled())

        kernel_main(main())
    finally:
        sos.scheduler.RAISE_UNAWAITED_SCHEDULED_EXCEPTIONS = True


@pytest.mark.kernel
async def test_noop_coroutine():
    pass


@pytest.mark.kernel
async def test_simple_service_call(services):
    simple_a_id, _ = services
    assert (await A(simple_a_id).inc(4)) == 5


@pytest.mark.kernel
async def test_service_call_makes_service_call(services):
    _, outsource_a_id = services
    assert (await A(outsource_a_id).inc(4)) == 5


@pytest.mark.kernel
async def test_service_calls_can_recursively_make_service_calls():
    # since it's the only backend, None means call itself
    # what we're testing here is that we can make an abitrary number of nested system calls
    await Services().register_backend(
        A,
        OutsourceA,
        OutsourceA.Args(None),
    )
    assert (await A().triangle(10)) == 55


# This isn't local to test_clientmethod for some silly reasons related to
# ServiceMeta.SERVICES being global, pytest running tests in a single interpreter instance,
# and test_remote.py therefor sometimes trying to pickle Service types defined in other
# test cases :)
class B(Service):
    async def inc(self, x: int) -> int:
        pass

    @service.clientmethod
    async def client_inc(self, x: int) -> int:
        return x + 1


@pytest.mark.kernel
async def test_clientmethod():

    with pytest.raises(ServiceNotFound):
        await B().inc(0)
    assert (await B().client_inc(0)) == 1


@pytest.mark.kernel
async def test_gather(services):
    simple, outsourced = services
    assert (5, 55) == await service.gather(
        A(simple).inc(4),
        A(outsourced).triangle(10),
    )


@pytest.mark.kernel
async def test_gather_delayed_execution(services):
    simple, _ = services
    assert 55 == sum(await service.gather(*(map(A(simple).inc, range(10)))))


@pytest.mark.skip
@pytest.mark.kernel
async def test_asyncio_gather_delayed_execution(services):
    simple, _ = services
    assert 55 == sum(await asyncio.gather(*(map(A(simple).inc, range(10)))))


@pytest.mark.kernel
async def test_scheduled():
    ran = False

    async def run_scheduled():
        nonlocal ran
        assert await Services().health_check()
        ran = True

    token = await service.schedule(run_scheduled())
    # This test is a bit of a race condition with the scheduler,
    # but I think it's also a pretty reasonable assertion about
    # how things _should_ work ideally
    assert not ran

    await token
    assert ran


@pytest.mark.kernel
async def test_scheduled__wait_on_random_token():
    with pytest.raises(KeyError):
        await AwaitScheduled(ScheduleToken())


@pytest.mark.kernel
async def test_asyncio():
    await asyncio.sleep(0.01)


@pytest.mark.kernel
async def test_asyncio__run_in_executor():
    lock = threading.Lock()

    ran = False

    def executed():
        nonlocal ran
        with lock:
            ran = True

    with lock:
        future = asyncio.get_running_loop().run_in_executor(
            executor=None, func=executed
        )
        assert not ran

    await future
    assert ran


@pytest.mark.kernel
async def test_asyncio__create_task():
    lock = asyncio.Lock()

    ran = False

    async def coro():
        nonlocal ran
        async with lock:
            ran = True

    async with lock:
        future = asyncio.get_running_loop().create_task(coro())
        assert not ran

    await future
    assert ran


@pytest.mark.kernel
async def test_asyncio__create_task_can_make_service_calls():
    lock = asyncio.Lock()

    ran = False

    async def coro():
        nonlocal ran
        async with lock:
            ran = await Services().health_check()

    async with lock:
        future = asyncio.get_running_loop().create_task(coro())
        assert not ran

    await future
    assert ran


@pytest.mark.kernel
async def test_asyncio__create_task_can_schedule_callbacks():
    lock = asyncio.Lock()
    callback_lock = asyncio.Lock()

    ran = False
    callback_ran = False

    async def coro():
        nonlocal ran
        async with lock:
            ran = True
        return ran

    def callback(fut):
        try:
            nonlocal callback_ran
            callback_ran = True
            assert ran
            assert fut.result()
        finally:
            callback_lock.release()

    async with lock:
        future = asyncio.get_running_loop().create_task(coro())
        assert not ran
        await callback_lock.acquire()
        future.add_done_callback(callback)

    await future
    assert ran
    async with callback_lock:
        assert callback_ran


@pytest.mark.kernel
async def test_asyncio__create_task_callback_on_raising_coro():
    lock = asyncio.Lock()
    callback_lock = asyncio.Lock()

    ran = False
    callback_ran = False

    async def coro():
        nonlocal ran
        async with lock:
            ran = True
        raise CustomError()

    def callback(fut):
        try:
            nonlocal callback_ran
            callback_ran = True
            assert ran
            with pytest.raises(CustomError):
                assert fut.result()
        finally:
            callback_lock.release()

    async with lock:
        future = asyncio.get_running_loop().create_task(coro())
        assert not ran
        await callback_lock.acquire()
        future.add_done_callback(callback)

    await future
    assert ran
    async with callback_lock:
        assert callback_ran


@pytest.mark.kernel
async def test_call_service__no_backend():
    with pytest.raises(ServiceNotFound):
        await A().inc(0)


@pytest.mark.kernel
async def test_call_service__no_backend_with_service_id(services):
    service_id = "not a service ID!"
    with pytest.raises(ServiceNotFound):
        await A(service_id).inc(0)


@pytest.mark.kernel
async def test_yield_non_service_call():
    class CustomSystemCall(AwaitScheduled):
        @property
        def token(self):
            raise RuntimeError("HAHA I BROKE YOUR KERNEL")

        @token.setter
        def token(self, new_token):
            pass

    with pytest.raises(TypeError) as excinfo:
        await CustomSystemCall(ScheduleToken())

    assert "Coroutine yielded non-ServiceCall" in str(excinfo.value)


@pytest.mark.kernel
async def test_access_private_endpoint():
    class AWithPrivate(A.Backend):
        async def private(self):
            return "secret"

    with pytest.raises(AttributeError):
        await A().private()

    with pytest.raises(ServiceHadNoMatchingEndpoint):
        await ServiceCall(current_execution_context(), A, None, "private", (), {})


@pytest.mark.skip
@pytest.mark.kernel
async def test_service_call_delayed_execution_doesnt_leak_execution_context(services):
    # In this test we're trying to run some code that would execute in
    # more than one execution context, in a single await call. We want to verify
    # That each part runs in the correct context, and that eg. it isn't instead
    # scheduled (that might just be easier though) or all run by the same user.
    # There's probably more direct ways we need to be testing each of these things.
    eca = ExecutionContext(User("a"), root=Path("/sandbox"))
    ecb = ExecutionContext(User("b"))

    class BOnly(A.Backend):
        async def inc(self, x):
            if current_execution_context().user.name != "b":
                raise Exception
            return x + 1

    simple, _ = services
    b_only = Services().register_backend(A, BOnly)

    # Currently can't even apply awaitable fns :)
    async def b_inc(x: int) -> int:
        with ecb.active():
            return await A(b_only).inc(x)

    with eca.active():
        assert 3 == await A(simple).inc(1).apply(b_inc)
