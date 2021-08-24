import asyncio
import dataclasses
import pytest

from sos import service
from sos.kernel_main import kernel_main
from sos.service import Service
from sos.services import Services


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
async def test_service_calls_can_be_awaited_simultaneously_with_gather(services):
    # TODO: service_result.gather cheats right now :)
    # This is actually closer to working than I thought o.o
    # We _probably_ don't want to use asyncio.gather because it has a bunch
    # of logic that care about event loops and Tasks and such. HOWEVER
    # digging into it revealed more details about how the event loop works,
    # and ServiceCall / ServiceResult actually share _a lot_ of concepts with
    # asyncio.futures.Future. I should read that code more carefully; it's very
    # possible that the right way to implement most of the "real" code eg.
    # waiting on sockets, etc. is with Futures, since it looks like they _can_
    # be yielded up to the event loop.
    simple, outsourced = services
    assert [5, 55] == await service.gather(
        A(simple).inc(4),
        A(outsourced).triangle(10),
    )


# TODO: test awaiting on a `ServiceResultApply` which is running a different service call
#       which runs in a different ExecutionContext and validate that ECs apply properly.
