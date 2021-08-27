import asyncio
import dataclasses
from pathlib import Path
from sos.service.service import AwaitScheduled, ScheduleToken, ServiceNotFound
import pytest

from sos import service
from sos.execution_context import ExecutionContext, User, current_execution_context
from sos.kernel_main import ServiceHadNoMatchingEndpoint, kernel_main
from sos.service import Service, ServiceCall
from sos.services import Services


# TODO:
#  - move many of these tests to test_service.py or test_services.py


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


def test_kernel_tests_execute():
    # Normally doesn't return anything; verify that an exception
    # will propogate up above the kernel

    class CustomError(Exception):
        pass

    async def main():
        raise CustomError

    loop = asyncio.get_event_loop()
    with pytest.raises(CustomError):
        loop.run_until_complete(kernel_main(main()))


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
    outsource_a_id = await Services().register_backend(
        A,
        OutsourceA,
        OutsourceA.Args(None),
    )
    assert (await A().triangle(10)) == 55


@pytest.mark.kernel
async def test_clientmethod():
    class B(Service):
        async def inc(self, x: int) -> int:
            pass

        @service.clientmethod
        async def client_inc(self, x: int) -> int:
            return x + 1

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
    assert 55 == await service.gather(*(map(A(simple).inc, range(10)))).apply(sum)


def test_scheduled():
    ran = False

    async def run_scheduled():
        nonlocal ran
        assert await Services().health_check()
        ran = True

    async def main():
        await service.schedule(run_scheduled())
        # This may fail; we don't make guarantees that it doesn't
        # run immediately.
        assert not ran

    loop = asyncio.get_event_loop()
    loop.run_until_complete(kernel_main(main()))

    assert ran


@pytest.mark.kernel
async def test_scheduled__wait_on_random_token():
    with pytest.raises(KeyError):
        await AwaitScheduled(ScheduleToken())


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
        def token(self):
            pass

    with pytest.raises(TypeError):
        await CustomSystemCall(ScheduleToken())


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

    ### Currently can't even apply awaitable fns :)
    async def b_inc(x: int) -> int:
        with ecb.active():
            return await A(b_only).inc(x)

    with eca.active():
        assert 3 == await A(simple).inc(1).apply(b_inc)


# TODO: test awaiting on a `ServiceResultApply` which is running a different service call
#       which runs in a different ExecutionContext and validate that ECs apply properly.
