import asyncio
from collections import defaultdict, deque
import functools
import traceback
from typing import Coroutine, Iterable, NamedTuple, Tuple, Union
from weakref import WeakKeyDictionary, WeakSet

from .execution_context import ExecutionContext, current_execution_context
from .service.service_call import AwaitScheduled, ScheduleToken


# TODO: don't busyloop when there's nothing to do :P
# TODO: unit test for CancelledError
# TODO: does asyncio.gather work naturally in place of service.gather?
# TODO: figure out what things can and can't be weakref'd that might matter
#       1. async_gen_asend (aka __anext__)


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

    def verify_waited_on(self):
        if not self.waited_on:
            if RAISE_UNAWAITED_SCHEDULED_EXCEPTIONS:
                raise self._exception
            print("Scheduled coroutine threw exception never surfaced")
            exc = self._exception
            traceback.print_exception(type(exc), exc, exc.__traceback__)
            self.waited_on = True

    def __del__(self):
        self.verify_waited_on()

    def __hash__(self):
        return id(self)


class ScheduledFuture(asyncio.Future):
    # Awkwardly, schedules itself :P However this shouldn't be created outside of
    # asyncio_task_factory anyway.
    def __init__(self, coro, ec, scheduler):
        self.coro = coro
        self.token = scheduler.schedule_now(self.run_and_resolve(), ec)
        self.scheduler = scheduler
        super().__init__()

    def add_done_callback(self, fn) -> None:
        @functools.wraps(fn)
        async def callback():
            return fn(self)

        ec = current_execution_context()
        self.scheduler.wait_on_schedule_token(callback(), ec, self.token)

    async def run_and_resolve(self):
        # must return None because the output is passed to .send on fresh coroutines
        try:
            self.set_result(await self.coro)
        except BaseException as e:
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
        # Helps us tracking running the event loop in between other scheduled work
        self._asyncio_loop_scheduled = False

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

    def surface_orphaned_exceptions(self):
        """Yell about any exceptions which were raised by scheduled coroutines but
        whose results were never awaited on anywhere."""
        for exception in self.scheduled_raised_exceptions:
            exception.verify_waited_on()

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
        try:
            self.resolve(f, f.exception() or f.result(), bool(f.exception()))
        except asyncio.CancelledError as ce:
            self.resolve(f, ce, True)
        finally:
            self.waiting_futures.remove(f)

    async def run_asyncio_loop_once(self):
        # The loop won't "run" if there's already one "running", but eg. asyncio.create_task
        # checks for the "running" loop :P
        asyncio.events._set_running_loop(None)
        self.asyncio_loop.call_soon(self.asyncio_loop.stop)
        self.asyncio_loop.run_forever()
        asyncio.events._set_running_loop(self.asyncio_loop)
        self._asyncio_loop_scheduled = False

    # ----------------- scheduler main loop ------------------------------------------------

    def __iter__(self) -> Iterable[Scheduled]:
        """Loop through the ready scheduled coroutines until their are none.
        Unless re-scheduled, yielded coroutines and results are removed from the queue.
        """
        loop_ec = current_execution_context()
        asyncio.set_event_loop(self.asyncio_loop)
        asyncio.events._set_running_loop(self.asyncio_loop)
        try:
            while self.ready or self.waiting or self.waiting_futures:
                if self.ready:
                    yield self.ready.popleft()
                if self.waiting_futures and not self._asyncio_loop_scheduled:
                    self._asyncio_loop_scheduled = True
                    self.schedule_now(self.run_asyncio_loop_once(), loop_ec)
        finally:
            asyncio.events._set_running_loop(None)
            asyncio.set_event_loop(None)
