from pathlib import Path

from sos.kernel_main import kernel_main
from sos.services import Services
from sos.services.files import Files, ProxyFilesystem
from sos.services.logs import Logs, StdoutLogs
from sos.services.shell import Shell, SimpleShellBackend

LOCAL_ROOT = Path(".sos-hard-drive")


async def run_shell():
    # TODO: we need a cleaner way to exit. We should be able to
    #       shut down the services, or call shutdown to shut down
    #       all services.
    await Services().register_backend(Logs, StdoutLogs)
    await Services().register_backend(
        Files,
        ProxyFilesystem,
        ProxyFilesystem.Args(local_root=LOCAL_ROOT),
    )
    await Services().register_backend(
        Shell, SimpleShellBackend, SimpleShellBackend.Args(2222)
    )


if __name__ == "__main__":
    kernel_main(run_shell())
