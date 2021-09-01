import asyncio
import dataclasses
from typing import Coroutine

from .execution_context import ExecutionContext, current_execution_context
from .scheduler import RaisedResult, Scheduler
from .service.service import (
    AwaitScheduled,
    Error,
    ServiceCall,
    Schedule,
    ServiceCallBase,
    ServiceHadNoMatchingEndpoint,
    current_call,
)
from .services import Services
from .services.services import TheServicesBackend


# TODO:
#   - when an exception is thrown, un-awaited tasks leak (should be cancelled)
#   - implement pre-emption, allowing on a time or certain events to stop the
#       current task and move it back to the `ready` state
#   - can we use contextvars to eliminate the need to pass ExecutionContext everywhere?
#   - schedule should put the current task ahead of the task it just scheduled in queue
#   - log in RaisedResult.__del__
#   - what happens if a scheduled asyncio.Future raises?


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
    if service_call.endpoint not in service_call.service.__endpoints__:
        raise ServiceHadNoMatchingEndpoint(service_call.service, service_call.endpoint)

    backend = services.get_backend(service_call.service, service_call.service_id)
    endpoint = getattr(backend, service_call.endpoint)
    with requested_ec.active(), current_call(backend, service_call.endpoint):
        return await endpoint(*service_call.args, **service_call.kwargs)


async def handle_scheduled(ec: ExecutionContext, scheduled: Schedule) -> any:
    requested_ec = scheduled.execution_context
    validate_execution_context(ec, requested_ec)
    with requested_ec.active():
        return await scheduled.awaitable


@dataclasses.dataclass
class Kernel:
    services: Services.Backend
    root_ec: ExecutionContext
    scheduler: Scheduler

    def schedule_service_call(
        self,
        coro: Coroutine,
        service_call: ServiceCallBase,
        ec: ExecutionContext,
    ):
        """Schedule a ServiceCall object with the Scheduler. Different ServiceCall
        classes have different ways they interact with the scheduler."""
        # Do explicit type checking here; these are objects passed by user code
        # and we don't want to just execute arbitrary code here.
        if type(service_call) is ServiceCall:
            service_call_coro = handle_service_call(ec, self.services, service_call)
            # Mark the coro as waiting on the service call and schedule service call
            self.scheduler.wait_on(coro, ec, service_call_coro)
            self.scheduler.schedule_now(service_call_coro, ec)

        elif type(service_call) is Schedule:
            token = self.scheduler.schedule_now(handle_scheduled(ec, service_call), ec)
            self.scheduler.schedule_now(coro, ec, token)

        elif type(service_call) is AwaitScheduled:
            self.scheduler.wait_on_schedule_token(coro, ec, service_call.token)

        elif isinstance(service_call, asyncio.Future):
            self.scheduler.wait_on_asyncio_future(coro, ec, service_call)

        elif service_call is None:
            # The coroutine signaled to hand control back to the event loop; common
            # in asyncio futures
            self.scheduler.schedule_now(coro, ec)

        else:
            # LOL don't call __repr__ or __str__ on untrusted objects
            exc = RaisedResult(
                self.scheduler,
                TypeError(f"Coroutine yielded non-ServiceCall {type(service_call)}"),
            )
            self.scheduler.schedule_now(coro, ec, exc, True)

    def main(self, main: Coroutine) -> any:
        """Orchestrates a program (main), allowing it to yield ServiceCall objects,
        which are then executed and the results sent back to the coroutine.
        Also manages execution context including validation and security checks,
        and manages looking up (and possibly starting) service backends.

        Make sure to use fine grained errors.
        Also, tracebacks are good! System should be developer friendly,
        err on the side of more info even if it exposes system internals.
        """
        # Schedule main to run as root.
        self.scheduler.schedule_now(main, self.root_ec)

        # Iterate over ready tasks in the scheduler. If they `raise` we, we keep the
        # result to `.throw` to any waiting tasks, otherwise we pass it via `.send`.
        for coro, ec, result, threw in self.scheduler:
            # Execute the coroutine until it yields a ServiceCall or Future object.
            # When send or throw raise StopIteration, the coroutine has exited.
            try:
                if threw:
                    exc = result.exception
                    service_call = coro.throw(type(exc), exc.args, exc.__traceback__)
                else:
                    service_call = coro.send(result)
            except StopIteration as e:
                self.scheduler.resolve(coro, e.value, threw=False)
            except Exception as e:
                if coro is main:
                    raise e
                self.scheduler.resolve(coro, e, threw=True)
            else:
                self.schedule_service_call(coro, service_call, ec)


def kernel_main(main: Coroutine) -> any:
    services = TheServicesBackend()
    services.register_backend_instance(Services, services, "highlander")
    kernel = Kernel(
        services=services,
        root_ec=current_execution_context(),
        scheduler=Scheduler(),
    )

    return kernel.main(main)
