import asyncio
from collections import defaultdict, deque
import dataclasses
import functools
from os import execlp
import traceback
from typing import Coroutine, Iterable, NamedTuple, Tuple, Union
from weakref import WeakKeyDictionary, WeakSet

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
    current_call,
)
from .services import Services
from .services.services import TheServicesBackend
from .util.coro import async_yield


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


# use a namedtuple here because we want to unpack them in kernel_main
class Scheduled(NamedTuple):
    coro: Coroutine
    execution_context: ExecutionContext
    result: any = None
    threw: bool = False


# Strict setting, very useful for unit testing.
RAISE_UNAWAITED_SCHEDULED_EXCEPTIONS = False


class RaisedResult:
    """This wraps exceptions raised by tasks so we can explicitly track whether
    they were ever accessed. If their deleter runs without raising, we know
    we leaked an exception and want to report it. We also can't trust the deleter
    to run before the system exits, so we register with
    scheduler.scheduled_raised_exceptions to give the kernel a hook to manually
    check for uncaught exceptions."""

    waited_on: bool = False

    def __init__(self, scheduler: "Scheduler", exception: Exception):
        self._exception = exception
        self.scheduler = scheduler
        self.scheduler.scheduled_raised_exceptions.add(self)

    @property
    def exception(self):
        if not self.waited_on:
            self.waited_on = True
            self.scheduler.scheduled_raised_exceptions.remove(self)
        return self._exception

    def __del__(self):
        if not self.waited_on:
            if RAISE_UNAWAITED_SCHEDULED_EXCEPTIONS:
                raise self._exception
            print("Scheduled coroutine threw exception never surfaced")
            exc = self._exception
            traceback.print_exception(type(exc), exc, exc.__traceback__)

    def __hash__(self):
        return id(self)


def ascoro(fn):
    @functools.wraps(fn)
    async def coro(*args, **kwargs):
        await async_yield
        return fn(*args, **kwargs)

    return coro


class ScheduledFuture(asyncio.Future):
    # Awkwardly, schedules itself :P However this shouldn't be created outside of
    # asyncio_task_factory anyway.
    def __init__(self, coro, ec, scheduler):
        self.coro = coro
        self.token = scheduler.schedule_now(self.run_and_resolve(), ec)
        self.scheduler = scheduler
        super().__init__()

    def add_done_callback(self, fn) -> None:
        coro = ascoro(fn)(self)
        coro.send(None)
        self.scheduler.wait_on_schedule_token(
            coro, current_execution_context(), self.token
        )

    async def run_and_resolve(self):
        try:
            self.set_result(await self.coro)
        except Exception as e:
            self.set_exception(e)

    def __await__(self):
        return (yield AwaitScheduled(self.token))


Result = Tuple[Union[any, RaisedResult], bool]
WaitingTasks = list[Tuple[Coroutine, ExecutionContext]]


class Scheduler:
    """Currently a cooperative scheduler. `ready` contains ready tasks in order, while
    `waiting` tracks the dependency graph of tasks. When a task completes via `resolve`,
    mark its dependent tasks as ready.

    Scheduler / Asyncio interop
        The Kernel + Scheduler together are basically an asyncio EventLoop, with a very
        pared down feature set. It would be fun to implement everything ourselves within
        this new framework, and maybe that's eventually in the cards, but for now there's
        _many_ practical utilities that have been built assuming the asyncio primitives,
        so we want to be able to interop with asyncio Futures also.

        The primary modes of interop with asyncio for supporting most asyncio operations
        (at least all I've tried so far) are

            1. __await__ yields an object produced by asyncio.create_task
            2. __await__ yields an asyncio.Future produced by loop.create_future
            3. __await__ yields an asyncio.Future created by wrapping a concurrent.Future

        For 1., we use loop.set_task_factory to return a Future object that manages
        execution through our scheduler (ScheduledFuture).

        For 2. and 3. we currently defer to a managed instance of an asyncio event loop.
    """

    def __init__(self):
        # Queues managing ready and waiting tasks
        self.ready = deque[Scheduled]()
        self.waiting = defaultdict[any, WaitingTasks](list)
        self.waiting_futures: set[asyncio.Future] = set()

        # Weak key data structures for tracking results that may ever be used again.
        self.schedule_tokens = WeakKeyDictionary[ScheduleToken, Coroutine]()
        self.scheduled_results = WeakKeyDictionary[Coroutine, Result]()
        self.scheduled_raised_exceptions = WeakSet[RaisedResult]()

        self.asyncio_loop: asyncio.BaseEventLoop = asyncio.new_event_loop()
        self.asyncio_loop.set_task_factory(self.asyncio_task_factory)

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
        self, coro: Coroutine, ec: ExecutionContext, waiting_on: Coroutine
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
        try:  # weak refs, so don't allow a GC race condition
            self.wait_on(coro, ec, self.schedule_tokens[token])
        except KeyError as e:
            self.schedule_now(coro, ec, RaisedResult(self, e), True)

    def resolve(self, coro: Coroutine, value: any, threw: bool = False) -> None:
        """Mark a coroutine as resolved. Move others waiting on it to ready."""
        value = RaisedResult(self, value) if threw else value

        for waiting, ec in self.waiting.pop(coro, ()):
            self.schedule_now(waiting, ec, value, threw=threw)

        # Always store results; it's a weak key dict so if nothing's waiting they'll be GCed later
        self.scheduled_results[coro] = (value, threw)

    # -------------- asyncio loop interop -----------------------------------------------

    def asyncio_task_factory(self, loop, coro):
        # TODO: do I need to do any EC validation? I think so but it's awkward to get
        #       the "current" ec vs "requested" ec :P These need to be separate contextvars
        return ScheduledFuture(coro, current_execution_context(), self)

    def wait_on_asyncio_future(
        self, coro: Coroutine, ec: ExecutionContext, future: asyncio.Future
    ):
        """Handle asyncio futures, ie. normal async functions defined in asyncio
        or 3rd party libraries. Schedule them in a managed event loop, and
        re-schedule the awaiting coroutine when it completes."""
        future.add_done_callback(self.resolve_asyncio_future)
        self.waiting[future].append((coro, ec))
        self.waiting_futures.add(future)

    def resolve_asyncio_future(self, f: asyncio.Future):
        self.resolve(f, f.exception() or f.result(), bool(f.exception()))
        self.waiting_futures.remove(f)

    def run_asyncio_loop_once(self):
        # The loop won't "run" if there's already one "running", but eg. asyncio.create_task
        # checks for the "running" loop :P
        asyncio.events._set_running_loop(None)
        self.asyncio_loop.call_soon(self.asyncio_loop.stop)
        self.asyncio_loop.run_forever()
        asyncio.events._set_running_loop(self.asyncio_loop)

    # ----------------- scheduler main loop ------------------------------------------------

    def __iter__(self) -> Iterable[Scheduled]:
        """Loop through the ready scheduled coroutines until their are none.
        Unless re-scheduled, yielded coroutines and results are removed from the queue.
        """
        asyncio.set_event_loop(self.asyncio_loop)
        asyncio.events._set_running_loop(self.asyncio_loop)
        try:
            while True:
                if self.ready:
                    yield self.ready.popleft()
                elif self.waiting_futures:
                    # It's probably not great to only run the event loop when there's no other
                    # ready tasks, since then we can make the kernel never schedule an event with
                    # while True: async_yield
                    # but we could also break the kernel with while True: pass so *shrug*
                    self.run_asyncio_loop_once()
                elif not self.waiting:
                    return
        finally:
            asyncio.events._set_running_loop(None)
            asyncio.set_event_loop(None)
            for exception in self.scheduled_raised_exceptions:
                exception.__del__()


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
