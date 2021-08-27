import dataclasses
import inspect
from pathlib import Path
import time
from typing import Optional

from sos.execution_context import User, current_execution_context
from sos.service import Service, clientmethod
from sos.service.service import ServiceBackendBase
from sos.services.files import Files


# TODO
#   - Timestamp type
#   - some basic querying :)
#   - using Files probably won't work long term
#       - what if a filesystem wants to log?
#       - what if we want to log all ServiceCalls from the kernel?
#   - log formatters for output


Timestamp = float


@dataclasses.dataclass
class LogLine:
    user: User
    path: Path
    ts: Timestamp
    file: str
    lineno: int
    service: Optional[type[Service]] = None
    backend: Optional[type[Service.Backend]] = None
    service_id: Optional[str] = None
    endpoint: Optional[str] = None
    data: dict[str, any] = dataclasses.field(default_factory=dict)


class Logs(Service):
    async def write_log(self, log: LogLine) -> None:
        pass

    async def query(self, **query: any) -> list[LogLine]:
        pass

    @clientmethod
    async def log(self, **data: any):
        """Log a structured log record, automatically tracking many details; see LogLine.
        Any keyword arguments passed to the log line will be stored as a structured log line,
        queryable, searchable, etc. To replicate simple application logging, you can eg.

        >>> await Logs().log(level="info", message="Something happened!")
        """
        # Introspect the stack and calling environment to populate all of the junk in LogLine
        ec = current_execution_context()
        ts = time.time()
        calling_frame = inspect.currentframe().f_back
        filename = calling_frame.f_code.co_filename
        lineno = calling_frame.f_lineno

        # Find calling service if any
        frame = calling_frame
        while frame and (
            "self" not in frame.f_locals
            or not isinstance(frame.f_locals["self"], ServiceBackendBase)
        ):
            print(
                frame.f_code.co_filename,
                frame.f_code.co_name,
                frame.f_locals.get("self"),
            )
            frame = frame.f_back

        if frame:
            backend_instance = frame.f_locals["self"]
            service_info = dict(
                service=backend_instance.interface,
                backend=type(backend_instance),
                service_id=backend_instance.service_id,
                endpoint=frame.f_code.co_name,
            )
        else:
            service_info = {}

        await self.write_log(
            LogLine(
                ec.user, ec.full_path, ts, filename, lineno, **service_info, data=data
            )
        )


class DevNullLogs(Logs.Backend):
    async def write_log(self, log: LogLine) -> None:
        pass

    async def query(self, **query) -> list[LogLine]:
        return []


class StdoutLogs(Logs.Backend):
    async def write_log(self, log: LogLine) -> None:
        print(log)

    async def query(self, **query) -> list[LogLine]:
        await Logs().log(query_args=query)
        return []


class ProxyFSLogs(Logs.Backend):
    @dataclasses.dataclass
    class Args:
        local_log_file: Path

    def __init__(self, args):
        super().__init__(args)
        self._logfile = open(self.local_log_file, "a")

    async def write_log(self, log: LogLine) -> None:
        print(log, file=self._logfile)

    async def query(self, **query) -> list[LogLine]:
        return []


class FSLogs(Logs.Backend):
    @dataclasses.dataclass
    class Args:
        log_path: Path
        files_service_id: Optional[str] = None

    async def write_log(self, log: LogLine) -> None:
        await Files(self.args.files_service_id).append(self.args.log_path, log)

    async def query(self, **query) -> list[LogLine]:
        return []
