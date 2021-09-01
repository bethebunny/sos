import dataclasses
import inspect
from pathlib import Path
import time
from typing import Optional

from rich import print

from sos.execution_context import User, current_execution_context
from sos.service import ScheduleToken, Service, clientmethod, schedule
from sos.service import service
from sos.services.files import Files


# TODO
#   - Timestamp type
#   - some basic querying :)
#   - using Files probably won't work long term
#       - what if a filesystem wants to log?
#       - what if we want to log all ServiceCalls from the kernel?
#   - log formatters for output
#   - track host, calling executable in log lines
#   - distributed tracing tokens
#       - contextvars will be killer for this
#   - better log tracing for scheduled tasks
#   - better default formatter


Timestamp = float


@dataclasses.dataclass
class LogLine:
    user: User
    path: Path
    ts: Timestamp
    file: str
    lineno: int
    service: Optional[type[Service]] = type(None)
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
    async def log(self, /, _currentframe=None, **data: any) -> ScheduleToken:
        """Log a structured log record, automatically tracking many details; see LogLine.
        Any keyword arguments passed to the log line will be stored as a structured log line,
        queryable, searchable, etc. To replicate simple application logging, you can eg.

        >>> await Logs().log(level="info", message="Something happened!")
        >>> await log(level="info", message="Something happened!")

        If you want to write an API that wraps logs calls, use `_currentframe` to help the
        logging introspection log the correct file and lineno values.

        For instance, here is how you might implement an interface closer to logger/log4j:

        >>> def info(format, **format_params):
        ...     if current_level.get() < Level.INFO:
        ...         return async_pass
        ...     message = format.format(**format_params)
        ...     return log(level="info", message=message, _currentframe=inspect.currentframe())
        >>> await info("Some stuff: {thing}, {other}", thing=5, other=10)
        """
        # Introspect the stack and calling environment to populate all of the junk in LogLine
        ec = current_execution_context()
        calling_frame = (_currentframe or inspect.currentframe()).f_back

        log_line = LogLine(
            user=ec.user,
            path=ec.full_path,
            ts=time.time(),
            file=calling_frame.f_code.co_filename,
            lineno=calling_frame.f_lineno,
            data=data,
        )

        # Find calling service if any
        backend_instance, endpoint = service.current_call.get((None, None))
        if backend_instance:
            log_line = dataclasses.replace(
                log_line,
                service=backend_instance.interface,
                backend=type(backend_instance),
                service_id=backend_instance.service_id,
                endpoint=endpoint,
            )

        return await schedule(self.write_log(log_line))


def log(**data: any) -> ScheduleToken:
    """Sugar for Logs().log(**data)."""
    return Logs().log(_currentframe=inspect.currentframe(), **data)


@dataclasses.dataclass
class Formatter:
    format: str

    def format_line(self, line: LogLine) -> str:
        return self.format.format(**dataclasses.asdict(line))


class DevNullLogs(Logs.Backend):
    async def write_log(self, log: LogLine) -> None:
        pass

    async def query(self, **query) -> list[LogLine]:
        return []


class StdoutLogs(Logs.Backend):
    @dataclasses.dataclass
    class Args:
        formatter: Formatter = Formatter(
            "{ts} {file}:{lineno} {user[name]}@[{service.__name__}({service_id}).{endpoint}] -- {data}"
        )

    async def write_log(self, log: LogLine) -> None:
        print(self.args.formatter.format_line(log))

    async def query(self, **query) -> list[LogLine]:
        # TODO: this is just here for demonstration in shell.py
        await log(query_args=query)
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
