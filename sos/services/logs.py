import contextlib
import dataclasses
import enum
from pathlib import Path
import pickle
import time
from typing import Optional

from sos.execution_context import ExecutionContext, User, current_execution_context
from sos.service import Service


class Level(enum.Enum, int):
    DEBUG = 0
    INFO = 1
    WARN = 2
    ERROR = 3


# TODO: some fancy metaprogramming on Logs().log to grab the service, service_id and endpoint
# from the call stack; also to 


Timestamp = float


@dataclasses.dataclass
class LogLine:
    level: Level
    user: User
    path: Path
    ts: Timestamp
    service: Optional[Service] = None
    service_id: Optional[str] = None
    endpoint: Optional[str] = None
    data: dict[str, any] = dataclasses.field(default_factory=dict)


class Logs(Service):
    async def log(self, level: Level, **data: any): pass
    async def query(self, **query)