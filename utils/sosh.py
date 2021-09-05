from IPython.terminal.embed import embed

from sos.execution_context import current_execution_context
from sos.kernel_main import Kernel
from sos.scheduler import Scheduler
from sos.service.remote import Remote
from sos.services.services import RemoteServicesBackend


if __name__ == "__main__":
    services = RemoteServicesBackend(Remote.Args(("localhost", 2222)))
    kernel = Kernel(
        services=services,
        root_ec=current_execution_context(),
        scheduler=Scheduler(),
    )

    # Can't point to kernel.main directly; IPython tries to pickle the function
    # and event loops don't pickle well :/
    def run(coro):
        return kernel.main(coro)

    embed(colors="neutral", using=run)
