import collections
import dataclasses
import traceback
from typing import Coroutine, Iterable, NamedTuple
import weakref

from .execution_context import ExecutionContext, current_execution_context
from .service.service import (
    AwaitScheduled,
    Error,
    Service,
    ServiceCall,
    Schedule,
    ScheduleToken,
    ServiceCallBase,
    ServiceHadNoMatchingEndpoint,
)
from .services import Services
from .services.services import TheServicesBackend


# TODO:
#   - when an exception is thrown, un-awaited tasks leak (should be cancelled)
#   - implement pre-emption, allowing on a time or certain events to stop the
#       current task and move it back to the `ready` state
#   - can we use contextvars to eliminate the need to pass ExecutionContext everywhere?
#   - schedule should put the current task ahead of the task it just scheduled in queue


class InvalidExecutionContextRequested(Error):
    """The execution context requested in the service call asked for permissions it
    doesn't have."""


# use a namedtuple here because we want to unpack them in kernel_main
class Scheduled(NamedTuple):
    coro: Coroutine
    execution_context: ExecutionContext
    result: any = None
    threw: bool = False


@dataclasses.dataclass
class ThrewResult:
    _exception: Exception
    waited_on: bool = False

    @property
    def exception(self):
        self.waited_on = True
        return self._exception

    def __del__(self):
        if not self.waited_on:
            print("Scheduled coroutine threw exception never surfaced")
            exc = self._exception
            traceback.print_exception(type(exc), exc, exc.__traceback__)


class Scheduler:
    # Currently a cooperative scheduler.
    # `ready` contains ready tasks in order, while `waiting` tracks the dependency graph
    # of tasks. When a task completes via `resolve` or `threw`, mark its dependent tasks
    # as ready.

    def __init__(self):
        # Queues managing ready and waiting tasks
        self.ready = collections.deque()
        self.waiting = collections.defaultdict(collections.deque)

        # Weak dicts storing results for scheduled tasks
        # Once any tasks have dropped their tokens (or closed), the GC will delete them,
        # which will allow these results to be collected also.
        self.schedule_tokens = weakref.WeakKeyDictionary()
        self.scheduled_results = weakref.WeakKeyDictionary()

    def schedule_now(
        self,
        coro: Coroutine,
        ec: ExecutionContext,
        send_value: any = None,
        threw: bool = False,
    ) -> ScheduleToken:
        """Schedule a coroutine for execution, marked as ready.
        If `threw`, send_value should be an Exception, otherwise its value
        will be passed to coro.send.
        """
        token = ScheduleToken()
        self.ready.append(Scheduled(coro, ec, send_value, threw))
        self.schedule_tokens[token] = coro
        return token

    def wait_on(
        self,
        coro: Coroutine,
        ec: ExecutionContext,
        waiting_on: Coroutine,
    ) -> None:
        if waiting_on in self.scheduled_results:
            # We're awaiting on a coro that already finished
            self.schedule_now(coro, ec, *self.scheduled_results[waiting_on])
        else:
            self.waiting[waiting_on].append((coro, ec))

    def wait_on_schedule_token(
        self, coro: Coroutine, ec: ExecutionContext, token: ScheduleToken
    ) -> None:
        """Wait on a previously scheduled execution."""
        try:
            waiting_on = self.schedule_tokens[token]
        except KeyError as e:
            self.schedule_now(coro, ec, ThrewResult(e), True)
        else:
            self.wait_on(coro, ec, waiting_on)

    def resolve(self, coro: Coroutine, value: any, threw: bool) -> None:
        """Mark a coroutine as resolved. Move others waiting on it to ready."""
        # Wrap thrown exceptions to track whether they were ever surfaced
        value = ThrewResult(value) if threw else value

        for waiting, ec in self.waiting.pop(coro, ()):
            self.schedule_now(waiting, ec, value, threw=threw)

        # Always store results; it's a weak key dict so if nothing's waiting they'll be GCed later
        self.scheduled_results[coro] = (value, threw)

    def __iter__(self) -> Iterable[Scheduled]:
        """Loop through the ready scheduled coroutines until their are none.
        Unless re-scheduled, yielded coroutines and results are removed from the queue.
        """
        while True:
            if not self.ready:
                return
            yield self.ready.popleft()


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
    with requested_ec.active():
        return await endpoint(*service_call.args, **service_call.kwargs)


async def handle_scheduled(ec: ExecutionContext, scheduled: Schedule) -> any:
    requested_ec = scheduled.execution_context
    validate_execution_context(ec, requested_ec)
    with requested_ec.active():
        return await scheduled.awaitable


@dataclasses.dataclass
class Kernel:
    services: Service.Backend
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

        else:
            exc = TypeError(f"Coroutine yielded non-ServiceCall {service_call}")
            self.scheduler.schedule_now(coro, ec, exc, True)

    async def main(self, main: Coroutine) -> any:
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


async def kernel_main(main: Coroutine) -> any:
    services = TheServicesBackend()
    services.register_backend_instance(Services, services, "highlander")
    kernel = Kernel(
        services=services,
        root_ec=current_execution_context(),
        scheduler=Scheduler(),
    )
    return await kernel.main(main)
