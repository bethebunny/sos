from sos.service import Service
from sos.services import Services
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
    service_id = await Services().register_backend(
        Simple,
        Remote[Simple],
        Remote.Args("remote_id", remote_service_id=None),
    )
    await Services().register_backend(Simple, SimpleImpl, None)
    assert (await Simple(service_id).inc(4)) == 5


@pytest.mark.kernel
async def test_remote_fails_with_no_available_remote():
    service_id = await Services().register_backend(
        Simple,
        Remote[Simple],
        Remote.Args("remote_id", remote_service_id=None),
    )
    with pytest.raises(RuntimeError):
        await Simple(service_id).inc(4)
