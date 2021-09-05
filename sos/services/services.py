import collections
import dataclasses
import random
import re
from typing import Optional

from sos.service.service import Service, ServiceMeta, ServiceNotFound


# TODO:
#   - API for removing services
#   - Interface for services complaining / saying they're unhealthy / notifying of shutdown
#   - better docs en backend
#   - premade words list
#   - actually check that the passed backend implements the interface :P
#   - allow services to provide futures to wait on some state change
#   - RemoteServicesBackend
#       - share session tokens between remotes pointing at the same service
#       - scheduled schedules in the local kernel, not remotely
#       - host/port spec
#       - real auth
#   - Some notion of "core" services which are required to fully import the kernel
#       and simplify the import dependency graph


def _load_words(path: str = "/usr/share/dict/words"):
    valid_word_re = re.compile(r"^[a-z]{4,8}$")
    with open(path) as words:
        for word in map(lambda s: s.strip(), words):
            if valid_word_re.match(word):
                yield word


_SEMANTIC_HASH_DICT = list(_load_words())


def _random_word():
    return random.choice(_SEMANTIC_HASH_DICT)


def semantic_hash(len=2):
    return "-".join(_random_word() for _ in range(len))


@dataclasses.dataclass
class BackendRegistryRecord:
    service_id: str
    type: type[Service.Backend]
    args: Service.Backend.Args


class Services(Service):
    async def register_backend(
        self,
        service: type[Service],
        backend: type[Service.Backend],
        args: Optional[Service.Backend.Args] = None,
        service_id: Optional[str] = None,
    ) -> str:
        """Register a new Service implementation for a Service.
        This is the core way that new system components are added.
        If a service_id is not provided, a human-friendly one will be generated :)

        >>> await Services().register_backend(Files, InMemoryFilesystem)
        'fragrant-avatar'

        >>> await Services().register_backend(
        ...     Files,
        ...     ProxyFilesystem,
        ...     ProxyFilesystem.Args(local_root=Path(".sos-hard-drive")),
        ...     service_id="custom-service-id",
        ... )
        'custom-service-id'
        """

    async def list_services(self) -> list[Service]:
        """List available Service APIs. This will return any know Service interface,
        not only those that have backends registered.

        >>> await Services().list_services()
        [Service, Files, Services, Logs]
        """

    async def list_backends(
        self, service: type[Service]
    ) -> list[BackendRegistryRecord]:
        """Return a list of all registered backends for a given Service type, and their IDs.
        Useful for introspecting the system to find running services, and routing service calls.
        For instance it would be easy to build a load balancing client on top of this.

        >>> await Services().list_backends(Services)
        [BackendRegistryRecord(service_id="highlander", TheServicesBackend, Service.Backend.Args())]
        """

    async def shutdown_backend(self, service: type[Service], service_id: str):
        """Shut down and unregister a service backend instance."""


class TheServicesBackend(Services.Backend):
    def __init__(self):
        super().__init__()
        # for now there's no difference between "registered" and "running"
        self._running: dict[
            type[Service], dict[str, Service.Backend]
        ] = collections.defaultdict(dict)

    async def register_backend(
        self,
        service: type[Service],
        backend: type[Service.Backend],
        args: Optional[Service.Backend.Args] = None,
        service_id: Optional[str] = None,
    ) -> str:
        service_id = service_id if service_id is not None else semantic_hash(2)
        args = args if args is not None else backend.Args()
        backend_instance = backend(args)
        await backend_instance.__asyncinit__()
        self.register_backend_instance(service, backend_instance, service_id)
        return service_id

    async def list_services(self) -> list[Service]:
        return list(ServiceMeta.SERVICES)

    async def list_backends(
        self, service: type[Service]
    ) -> list[(str, type[Service.Backend], Service.Backend.Args)]:
        return [
            BackendRegistryRecord(service_id, type(backend), backend.args)
            for service_id, backend in self._running.get(service, {}).items()
        ]

    async def shutdown_backend(self, service: type[Service], service_id: str):
        print(self._running)
        backend_instance = self._running[service][service_id]
        try:
            await backend_instance.shutdown()
        finally:
            for service_implemented in service.__mro__:
                if issubclass(service_implemented, Service):
                    del self._running[service_implemented][service_id]

    def register_backend_instance(
        self, service: type[Service], instance: Service.Backend, service_id: str
    ):
        instance.service_id = service_id
        for service_implemented in service.__mro__:
            if issubclass(service_implemented, Service):
                # TODO: what if it's already running?
                self._running[service_implemented][service_id] = instance

    def get_backend(
        self, service: type[Service], service_id: Optional[str] = None
    ) -> Service.Backend:
        impls = self._running.get(service, {})
        if service_id is not None:
            if service_id not in impls:
                raise ServiceNotFound(
                    f"no {service} backend running with ID {service_id}"
                )
            return impls[service_id]
        else:
            if not impls:
                raise ServiceNotFound(f"no running backend found for {service}")
            return next(iter(impls.values()))
