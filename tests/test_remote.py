from sos.execution_context import current_execution_context
from sos.service import Service, ServiceService
from sos.service.remote import Remote

import pytest


class Simple(Service):
    async def inc(self, x: int) -> int:
        pass


class SimpleImpl(Simple.Backend):
    async def inc(self, x: int) -> int:
        return x + 1


@pytest.mark.kernel
async def test_remote_simple():
    service_id = await ServiceService().register_backend(
        Simple,
        Remote[Simple],
        Remote.Args("remote_id", remote_service_id=None),
    )
    fake_remote_service_id = await ServiceService().register_backend(
        Simple, SimpleImpl, None
    )
    print(service_id)
    assert (await Simple(service_id).inc(4)) == 5
