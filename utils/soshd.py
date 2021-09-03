from pathlib import Path
from sos.service.service import Service

from sos.kernel_main import kernel_main
from sos.services import Services
from sos.services.authentication import Authentication, OpenSeason
from sos.services.files import Files, ProxyFilesystem
from sos.services.logs import Logs, StdoutLogs
from sos.services.remote_host import RemoteHostBackend

LOCAL_ROOT = Path(".sos-hard-drive")


# A demo kernel that we can connect to and execute remote kernel calls against


async def run_remote():
    await Services().register_backend(Logs, StdoutLogs)
    await Services().register_backend(
        Files,
        ProxyFilesystem,
        ProxyFilesystem.Args(local_root=LOCAL_ROOT),
    )
    await Services().register_backend(Authentication, OpenSeason)
    await Services().register_backend(
        Service, RemoteHostBackend, RemoteHostBackend.Args(port=2222)
    )


if __name__ == "__main__":
    kernel_main(run_remote())
