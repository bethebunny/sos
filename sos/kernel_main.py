import collections
from pathlib import Path
from typing import Coroutine, Iterable, Optional, Tuple

from .execution_context import ExecutionContext, current_execution_context
from .service import ServiceCall
from .services import Services
from .services.services import TheServicesBackend


# TODO: Errors should be nice, eg. "did you mean...?"
class Error(Exception):
    """Base service call error."""


class ServiceNotFound(Error):
    """Didn't find the service in the services lookup."""


class ServiceDidNotStart(Error):
    """There was a failure starting a backend for the service."""


class ServiceHadNoMatchingEndpoint(Error):
    """The service backend for that service didn't have the requested method endpoint."""


class InvalidExecutionContextRequested(Error):
    """The execution context requested in the service call asked for permissions it
    doesn't have."""


def validate_execution_context(ec: ExecutionContext, requested_ec: ExecutionContext):
    """Do security checks; if the requested execution context requests more permissions
    than the current one, reject it and raise an exception."""
    # this is critical path for system calls, and is a bit sloooow
    # 80/20 solution right now is to do a quick check here for the common case
    if ec is requested_ec or str(requested_ec.root).startswith(str(ec.root)):
        return
    # fast check failed, do a more thorough check
    # For now just checking that we're not breaking chroot.
    abs_root = ec.root.resolve()
    requested_root = requested_ec.root.resolve()
    if not requested_root.is_relative_to(abs_root):
        raise InvalidExecutionContextRequested(
            f"New root was not a subset of the old root: {requested_ec.root}"
        )


class Scheduler:
    # Currently a cooperative scheduler.
    # `ready` contains ready tasks in order, while `waiting` tracks the dependency graph
    # of tasks. When a task completes via `resolve` or `threw`, mark its dependent tasks
    # as ready. For now a task may only be waiting on 1 task at once.
    # TODO: allow waiting on multiple tasks simultaneously for `gather`
    # TODO: implement pre-emption, allowing on a time or certain events to stop the
    #       current task and move it back to the `ready` state
    # TODO: can we use contextvars to eliminate the need to pass ExecutionContext everywhere?

    def __init__(self):
        self.ready = collections.deque()
        self.waiting = collections.defaultdict(collections.deque)

    def schedule(
        self,
        coro: Coroutine,
        ec: ExecutionContext,
        waiting_on: Optional[Coroutine] = None,
    ) -> None:
        if waiting_on is None:
            self.ready.append((None, False, ec, coro))
        else:
            self.waiting[waiting_on].append((ec, coro))

    def resolve(self, coro: Coroutine, value: any) -> None:
        self.ready.extend(
            (value, False, ec, waiting) for ec, waiting in self.waiting.pop(coro, ())
        )

    def threw(self, coro: Coroutine, exc: Exception) -> None:
        self.ready.extend(
            (exc, True, ec, waiting) for ec, waiting in self.waiting.pop(coro, ())
        )

    def __iter__(self) -> Iterable[Tuple[any, bool, ExecutionContext, Coroutine]]:
        while True:
            if not self.ready:
                return
            yield self.ready.popleft()


async def handle_service_call(
    ec: ExecutionContext, services: Services.Backend, service_call: ServiceCall
) -> any:
    """
    1. validate and set the execution context
    2. find the backend service
    3. schedule and await on the service call
    4. save result to service_call_result
    """
    requested_ec = service_call.execution_context
    validate_execution_context(ec, requested_ec)
    # we guarantee that the Services will not make system calls
    backend = await services.get_backend(service_call.service, service_call.service_id)
    if not hasattr(backend, service_call.endpoint):
        raise ServiceHadNoMatchingEndpoint(service_call.service, service_call.endpoint)
    endpoint = getattr(backend, service_call.endpoint)
    with requested_ec.active():
        return await endpoint(*service_call.args, **service_call.kwargs)


async def kernel_main(main: Coroutine) -> any:
    """Orchestrates a program (main), allowing it to yield ServiceCall objects,
    which are then executed and the results sent back to the coroutine.
    Also manages execution context including validation and security checks,
    and manages looking up (and possibly starting) service backends.

    Make sure to use fine grained errors.
    Also, tracebacks are good! System should be developer friendly,
    err on the side of more info even if it exposes system internals.
    """
    # Set up the core system state: the services backend, scheduler, and root permissions.
    services = TheServicesBackend()
    root_ec = current_execution_context()
    scheduler = Scheduler()

    # Schedule main to run as root.
    scheduler.schedule(main, root_ec)

    # Iterate over ready tasks in the scheduler. If they `raise` we, we keep the
    # result to `.throw` to any waiting tasks, otherwise we pass it via `.send`.
    for result, threw, ec, coro in scheduler:
        # Execute the coroutine until it yields a ServiceCall or Future object.
        # When send or throw raise StopIteration, the coroutine has exited.
        try:
            if threw:
                exc = result
                service_call = coro.throw(type(exc), exc.args, exc.__traceback__)
            else:
                service_call = coro.send(result)
        except StopIteration as e:
            scheduler.resolve(coro, e.value)
        except Exception as e:
            if coro is main:
                raise e
            scheduler.threw(coro, e)
        else:
            service_call_coro = handle_service_call(ec, services, service_call)
            # Mark the coro as waiting on the service call and schedule service call
            scheduler.schedule(coro, ec, waiting_on=service_call_coro)
            scheduler.schedule(service_call_coro, ec)
