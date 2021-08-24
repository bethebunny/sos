import dataclasses
import functools
from typing import Optional, Type, TypeVar

from . import Service, ServiceService
from ..file_service import Files

S = TypeVar("S", bound=Service)


# We're kindof prototyping a Service decorator here
# so it makes sense that this will be a bit wonky at first :)
# Kindof what we want is a macro or cutpoint that lets us get
#   1. Type[Service]
#   2. endpoint
#   3. *args and **kwargs
# and then can wrap the call in some logic before it's executed.
# There _might_ be two different kinds of decorators; for instance
# I was mainly imagining "backend" decorators ie. ones that would just
# be normal python function wrappers over a real backend implementation
# but for instance Remote is more of a "backend" implementation itself
# that doesn't care about any particular backend details, but wants
# to defer to any backend implementation it finds with the remote
# specification. It's possible this is unique, ie. there's no other
# Remote-type things we want to do, and all others are "normal"
# backend-style decorators.


class Remote:
    """A backend type decorator that allows creating Remote implementations of services.

    For instance,

    >>> await ServiceService().register_backend(Files, Remote[Files], Remote.Args(ip_address))
    >>> await Files().list_directory()

    will let you use a `Files` service running at `ip_address`!

    For now it's not actually running remotely; mainly what's implemented is the Service wrapper
    paradigm. Instead it just looks for any other locally running backend and uses that :)

    In order to actually get this _fully_ working we need to figure out
        - How do we serialize types / ServiceCalls? (probably dill for now)
        - How do we allow exposing services to be registered for external use?
        - Some kind of scheduling to allow the remote to actually listen and respond to service calls :)
            Ideally this will not be _too_ different from how the kernel schedules calls anyway.
        - How do we package and evaluate a chain of service calls intended for the same service?
    """

    @dataclasses.dataclass
    class Args:
        remote_id: str
        remote_service_id: Optional[str] = None

    # TODO: probably making this use Generic smartly so I can reuse those tools
    _service_type: Type[Service]

    async def __call_remote(self, endpoint, args, kwargs) -> any:
        print(
            f"Calling remote {self.args.remote_id} service {self.args.remote_service_id} "
            f" endpoint {endpoint}({', '.join(map(repr, args))}{', ' if kwargs else ''}"
            f"{', '.join(f'{k}={v!r}' for k, v in kwargs.items())})"
        )
        # TODO: make this actually do a remote call :)
        services = await ServiceService().list_backends(self._service_type)
        for service_id, backend_type, _ in services:
            if not issubclass(backend_type, Remote):
                endpoint = getattr(self._service_type(service_id), endpoint)
                return await endpoint(*args, **kwargs)
        else:
            raise RuntimeError(
                f"No local service backend found to pretend for {self._service_type}"
            )

    # This should probably be cached.
    @classmethod
    def __class_getitem__(cls, item):
        if not issubclass(item, Service):
            raise TypeError(f"Remote must take a Service type parameter; got {item}.")

        def make_endpoint(endpoint):
            @functools.wraps(getattr(item, endpoint))
            async def remote_endpoint(self, *args, **kwargs):
                return await self.__call_remote(endpoint, args, kwargs)

            return remote_endpoint

        endpoints = {
            endpoint: make_endpoint(endpoint) for endpoint in item.__endpoints__
        }

        return type(
            f"Remote[{item.__name__}].Backend",
            (cls, item.Backend),
            {"_service_type": item, **endpoints},
        )
