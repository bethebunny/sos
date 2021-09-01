import asyncio
import dataclasses
from typing import Coroutine
import uuid

import dill

from sos.execution_context import ExecutionContext, User, current_execution_context
from sos.service import ScheduleToken, Service, schedule, current_call
from sos.services.logs import log

# TODO:
#   - clean up code
#   - clean up documentation

# The current Dockerfile is really not the normal Docker operating style.
# It's not an unreasonable way to run the kernel as a _single service_, but
# it's not really possible to communicate with the kernel once it's running.

# What we really want is the kernel to be the ENTRYPOINT to the docker service,
# and then to replace SHELL with a process that can communicate with the kernel.

# Ideally this should have some normal shell semantics, eg. `list` and `call` working
# as they currently do. It should also be able to register new backend services and
# schedule tasks on the main kernel. `dill` isn't unreasonable for this, but there's
# some complexity in cases where the `dill` code imports code that doesn't already
# exist in the kernel.

# Right now I'm thinking `shell.py` should be the SHELL, and `kernel_main.py` should
# be the ENTRYPOINT, but `shell.py` (or whatever replaces it) should communicate with
# the running kernel to run commands rather than running them in its own kernel.
# The easiest way to do that is probably to have a Shell service that does what shell.py
# currently does and then use simple IPC to to call them.

# Another option is to figure out how Remote calls are going to work, and have shell.py
# run its own kernel but register Remote services. I don't like that approach for
# practicality; Services would all appear as `Remote`, and would need to be indidually
# registered with the Shell too.
# Hmm what if the shell's kernel has Services registered as Remote?!
# That might work. Right now `schedule` doesn't actually use the Services backend, so
# it would still be scheduled "locally" ie. in the shell's kernel. We'd probably need
# the remote kernel to be running some kind of Shell service at minimum to be able to
# handle scheduling new tasks. Also `handle_service_call` uses `services.get_backend`
# so we'd also need to implement a special RemoteServices(Services.Backend).

# I think the thing that makes the most sense is to have a Shell service (or maybe
# a different name with less baggage) whose job is to expose the interface for
# remote operation, and then have `shell.py` be a normal python script which can
# interact with the Shell service (and locally maintains any shell-like state;
# from the server's perspective the connection is stateless and more like a rest API)


@dataclasses.dataclass(frozen=True, unsafe_hash=True)
class RemoteToken:
    token: uuid.uuid4 = dataclasses.field(default_factory=uuid.uuid4)


class RemoteScheduleToken(RemoteToken):
    pass


class LoginToken(RemoteToken):
    pass


class SessionToken(RemoteToken):
    pass


@dataclasses.dataclass
class Session:
    execution_context: ExecutionContext


class Shell(Service):
    async def authorize(self, user: User, login_token: LoginToken) -> SessionToken:
        pass

    # TODO: some way to specify root path / working directory for execution context
    async def run(self, session_token: SessionToken, coroutine: Coroutine) -> any:
        pass

    async def schedule(
        self, session_token: SessionToken, coroutine: Coroutine
    ) -> RemoteScheduleToken:
        pass

    async def wait_on_token(
        self, session_token: SessionToken, token: RemoteScheduleToken, can_drop=True
    ):
        pass

    async def fire_and_forget(
        self, session_token: SessionToken, coroutine: Coroutine
    ) -> None:
        pass


class SimpleShellBackend(Shell.Backend):
    @dataclasses.dataclass
    class Args:
        port: 2222

    def __init__(self, args):
        super().__init__(args)
        self._sessions: dict[SessionToken, Session] = {}
        self._remote_schedule_tokens: dict[RemoteScheduleToken, ScheduleToken] = {}

    async def __asyncinit__(self):
        server = await asyncio.start_server(
            self.handle_connection, "localhost", self.args.port
        )
        addr = server.sockets[0].getsockname()
        await log(serving_on=addr)
        self._serve_coro = self.serve(server)
        await schedule(self._serve_coro)

    # TODO: shutdown method which calls self._serve_coro.cancel()
    # TODO: if serve fails we should raise a bigger stink and at the very least unregister
    #       ourself with services or mark ourselves as broken
    # TODO: a better mechanism for scheduled tasks to complain / raise a stink / exit normally

    async def serve(self, server):
        try:
            async with server:
                await server.serve_forever()
        finally:
            self._healthy = False

    async def handle_connection(self, reader, writer):
        request = await reader.read()
        with current_call(self, "handle_connection"):
            try:
                session_token, endpoint, args = dill.loads(request)
                await log(session_token=session_token, endpoint=endpoint, args=args)
                if endpoint == "authorize":
                    result = await Shell().authorize(*args)
                else:
                    session = self._sessions[session_token]
                    await log(user=session.execution_context.user, endpoint=endpoint)
                    with session.execution_context.active():
                        result = await getattr(Shell(), endpoint)(*args)
                await log(result=result)
                writer.write(dill.dumps(result, byref=True))
            except Exception as e:
                # TODO: log stack traces
                await log(error=type(e), message=str(e))
                import traceback

                traceback.print_exc()
                writer.write(dill.dumps(e))
            finally:
                writer.close()
                await writer.wait_closed()
                await log(closed=True)

    async def authorize(self, user: User, login_token: LoginToken) -> SessionToken:
        await log(user=user)
        session_token = SessionToken()
        session = Session(current_execution_context().replace(user=user))
        self._sessions[session_token] = session
        return session_token

    async def run(self, coroutine: Coroutine) -> any:
        return await coroutine()

    async def schedule(self, coroutine: Coroutine) -> RemoteScheduleToken:
        token = await schedule(coroutine())
        remote_token = RemoteScheduleToken()
        self._remote_schedule_tokens[remote_token] = token
        return remote_token

    async def wait_on_token(
        self,
        remote_token: RemoteScheduleToken,
        can_drop=True,
    ):
        token = self._remote_schedule_tokens[remote_token]
        try:
            return await token
        finally:
            if can_drop:
                del self._remote_schedule_tokens[remote_token]

    async def fire_and_forget(self, coroutine: Coroutine) -> None:
        await schedule(coroutine())
