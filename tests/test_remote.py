from sos.service.service import ServiceNotFound
import threading
from sos.kernel_main import (
    Kernel,
    Scheduler,
    TheServicesBackend,
    current_execution_context,
)
from sos.service import Service
from sos.services import Services
from sos.services.authentication import Authentication, OpenSeason
from sos.services.files import Files
from sos.services.logs import Logs, StdoutLogs
from sos.services.remote_host import RemoteHostBackend
from sos.service.remote import Remote, RemoteServicesBackend
from sos.util.coro import async_yield

import pytest
from conftest import kernel


class Simple(Service):
    async def inc(self, x: int) -> int:
        pass


class SimpleImpl(Simple.Backend):
    async def inc(self, x: int) -> int:
        return x + 1


@pytest.fixture
def remote_kernel():
    services = TheServicesBackend()
    services.register_backend_instance(Services, services, "highlander")
    return Kernel(
        services=services,
        root_ec=current_execution_context(),
        scheduler=Scheduler(),
    )


async def start_remote(connection_info, listener_started, service_id_ref, shutdown):
    await Services().register_backend(Simple, SimpleImpl)
    await Services().register_backend(Logs, StdoutLogs)
    await Services().register_backend(Authentication, OpenSeason)
    service_id = await Services().register_backend(
        Service, RemoteHostBackend, RemoteHostBackend.Args(*connection_info)
    )

    # send service_id back to fixture and notify it that startup is complete
    service_id_ref.append(service_id)
    with listener_started:
        listener_started.notify()

    # Wait for the remote fixture to release the shutdown lock, passing priority back
    # to the kernel.
    while not shutdown.acquire(blocking=False):
        await async_yield

    # Shut down backend service
    await Services().shutdown_backend(Service, service_id)


@pytest.fixture
def remote(remote_kernel):
    # This is an awkward mix of threading and async because we're trying to coordinate
    # timing between event loops in two different threads, which is exactly the opposite
    # of how threading and async were designed to work :) but necessary for us to actually
    # test the interactions between two separately operating event loops.

    # release this to signal to the remote kernel to shut down
    shutdown = threading.Lock()
    shutdown.acquire()

    # coordinate to wait on thethe remote service ID
    listener_started = threading.Condition()
    service_id_ref = []

    connection_info = ("localhost", 2223)
    # Start the remote kernel
    remote = threading.Thread(
        target=remote_kernel.main,
        args=(
            start_remote(connection_info, listener_started, service_id_ref, shutdown),
        ),
    )

    with listener_started:
        remote.start()
        listener_started.wait()

    try:
        yield connection_info
    finally:
        # signal remote kernel to shut down
        shutdown.release()


@pytest.fixture
async def remote_simple(remote):
    service_id = await Services().register_backend(
        Simple,
        Remote[Simple],
        Remote.Args(remote, remote_service_id=None),
    )
    yield Simple(service_id)


@pytest.mark.kernel
async def test_remote_simple(remote_simple):
    assert (await remote_simple.inc(4)) == 5


@pytest.mark.kernel
async def test_remote_incorrect_service_id(remote):
    service_id = await Services().register_backend(
        Simple,
        Remote[Simple],
        Remote.Args(remote, remote_service_id="0001-bacon"),
    )
    with pytest.raises(ServiceNotFound):
        await Simple(service_id).inc(4)


@pytest.mark.kernel
async def test_remote_fails_with_no_available_remote(remote):
    service_id = await Services().register_backend(
        Files,
        Remote[Files],
        Remote.Args(remote, remote_service_id=None),
    )
    with pytest.raises(ServiceNotFound):
        await Files(service_id).list_directory()


async def ascoro(awaitable):
    return await awaitable


def test_remote_services_backend(remote):
    kernel = Kernel(
        RemoteServicesBackend(Remote.Args(remote)),
        current_execution_context(),
        Scheduler(),
    )

    assert Authentication in kernel.main(ascoro(Services().list_services()))

    services_backend = next(
        iter(kernel.main(ascoro(Services().list_backends(Services))))
    ).type
    assert services_backend is TheServicesBackend


# TODO:
#   - test full remote eg. RemoteServicesBackend
#   - test time travel for remote calls
