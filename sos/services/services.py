import collections
import random
import re
from typing import Optional

from sos.service.service import Service, ServiceMeta


def _load_words(path: str = "/usr/share/dict/words"):
    valid_word_re = re.compile(r"^[a-z]{4,7}$")
    with open(path) as words:
        for word in map(lambda s: s.strip(), words):
            if valid_word_re.match(word):
                yield word


_SEMANTIC_HASH_DICT = list(_load_words())


def _random_word():
    return random.choice(_SEMANTIC_HASH_DICT)


def semantic_hash(len=2):
    return "-".join(_random_word() for _ in range(len))


class Services(Service):
    async def register_backend(
        self,
        service: type[Service],
        backend: type[Service.Backend],
        args: Optional[Service.Backend.Args] = None,
        service_id: Optional[str] = None,
    ) -> str:
        pass

    async def list_services(self) -> list[Service]:
        pass

    async def list_backends(
        self, service: type[Service]
    ) -> list[(str, type[Service.Backend], Service.Backend.Args)]:
        pass


# @highlander
# It probably makes sense to be able to replace this too :)
# BUT NOT FOR NOW!
class TheServicesBackend(Services.Backend):
    def __init__(self):
        super().__init__()
        # self._registered: dict[dict[str, Service.Backend]] = collections.defaultdict(dict)
        # for now there's no difference between "registered" and "running"
        self._running: dict[
            type[Service], dict[str, Service.Backend]
        ] = collections.defaultdict(dict)
        self._running[Services]["highlander"] = self

    async def register_backend(
        self,
        service: type[Service],
        backend: type[Service.Backend],
        args: Optional[Service.Backend.Args] = None,
        service_id: Optional[str] = None,
    ) -> str:
        service_id = service_id if service_id is not None else semantic_hash(2)
        args = args if args is not None else backend.Args()

        impls = self._running[service]
        # TODO
        # if service_id in impls:
        #    raise
        impls[service_id] = backend(args)
        return service_id

    async def list_services(self) -> list[Service]:
        return list(ServiceMeta.SERVICES)

    async def list_backends(
        self, service: type[Service]
    ) -> list[(str, type[Service.Backend], Service.Backend.Args)]:
        return [
            (service_id, type(backend), backend.args)
            for service_id, backend in self._running.get(service, {}).items()
        ]

    # TODO: this needs to be privileged, this should only be executed by the kernel
    #       ... crazy idea, maybe there's a different service interface for it for
    #       users? or ACTUALLY since I'm not using the client in the kernel I can just
    #       not add it to the interface!!! genius
    #       however, the TODO here is now that the kernel doesn't validate system calls,
    #       so I can leak backends by constructing and yielding a SystemCall that runs
    #       a method that's not on the interface xD
    async def get_backend(
        self, service: type[Service], service_id: Optional[str] = None
    ) -> Service.Backend:
        impls = self._running.get(service, {})
        if service_id is not None:
            return impls[service_id]
        else:
            if not impls:
                # TODO error types
                raise RuntimeError(f"no running service found for {service}")
            return next(iter(impls.values()))
