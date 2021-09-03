import asyncio
import dataclasses
from typing import Optional

import dill

from sos.service import ScheduleToken, Service, ServiceCall, schedule, current_call
from sos.service.remote import (
    AuthenticationFailed,
    NoActiveSession,
    RemoteScheduleToken,
    Session,
    SessionToken,
)
from sos.services.authentication import Authentication
from sos.services.logs import log


# TODO:
#   - allowlist/denylist for which service calls are exposed remotely
#   - docs
#   - unit tests


def is_auth(service_call: ServiceCall) -> bool:
    return (
        service_call.service is Authentication
        and service_call.endpoint == "authenticate"
    )


class RemoteHostBackend(Service.Backend):
    @dataclasses.dataclass
    class Args:
        port: 2222

    _healthy: bool = False

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
        self._server = server
        await schedule(self.serve(server))

    # TODO: if serve fails we should raise a bigger stink and at the very least unregister
    #       ourself with services or mark ourselves as broken
    # TODO: a better mechanism for scheduled tasks to complain / raise a stink / exit normally
    # TODO: let services define dependencies, eg. we depend on Authentication.

    async def serve(self, server):
        try:
            async with server:
                self._healthy = True
                await server.serve_forever()
        except asyncio.CancelledError:
            pass
        finally:
            self._healthy = False

    async def shutdown(self):
        log()
        self._server.close()

    async def health_check(self):
        return self._healthy

    def validate_session_token(self, session_token) -> Optional[SessionToken]:
        return self._sessions.get(session_token)

    async def handle_connection(self, reader, writer):
        request = await reader.read()
        with current_call(self, "handle_connection"):
            try:
                session_token, service_call = dill.loads(request)
                await log(session_token=session_token, service_call=service_call)
                session = self.validate_session_token(session_token)
                if is_auth(service_call):
                    authentic = await service_call
                    if not authentic:
                        raise AuthenticationFailed(authentic)
                    result = SessionToken()
                    self._sessions[result] = Session(service_call.execution_context)
                elif not session:
                    raise NoActiveSession(session_token)
                else:
                    # TODO: what do we do with service_call.execution_context?
                    with session.execution_context.active():
                        result = await service_call
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
