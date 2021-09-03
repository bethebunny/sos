import enum
from typing import Union

from sos.service import Service


class FailedAuthenticationReason(enum.Enum):
    UNKNOWN = "UNKNOWN"

    def __bool__(self):
        return False


class Authentication(Service):
    async def authenticate(self, something) -> Union[bool, FailedAuthenticationReason]:
        pass


class OpenSeason(Authentication.Backend):
    async def authenticate(self, something) -> Union[bool, FailedAuthenticationReason]:
        return True
