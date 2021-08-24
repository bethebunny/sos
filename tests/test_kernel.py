import dataclasses
import pytest

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
        raise NotImplemented


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


# TODO: test awaiting on multiple service calls at the same time with a `gather`
# TODO: test awaiting on a `ServiceResultApply` which is running a different service call
#       which runs in a different ExecutionContext and validate that ECs apply properly.
