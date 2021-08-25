import contextlib
import dataclasses
from pathlib import Path
from typing import Optional


@dataclasses.dataclass
class User:
    name: str


ROOT = User("root")


# The user _should not_ be able to possibly use an execution context
# that's not a subset of the permissions of their own execution context.
# Obviously this implementation doesn't do that yet.
#
# For the intended api is eg.
# with current_execution_context().replace(root="/home/stef").active():
#     files = Files()
#
# There's soooo many ergonomic TODO here
#  - let ExecutionContext be used as a context manager
#  - don't have to use dataclasses.replace everywhere
#  - current_execution_context() is really wordy and we use it everywhere
@dataclasses.dataclass(frozen=True)
class ExecutionContext:
    user: User
    root: Path = Path("/")
    working_directory: Path = Path("/")
    # if sandbox, then by default activating will chroot
    sandbox: bool = True

    @property
    def full_path(self):
        return self.root / self.working_directory.relative_to("/")

    def replace(self, **kwargs):
        return dataclasses.replace(self, **kwargs)

    def nosandbox(self) -> "ExecutionContext":
        """An execution context which won't sandbox."""
        return self.replace(sandbox=False)

    def chroot(self) -> "ExecutionContext":
        return self.replace(root=self.full_path, working_directory=Path("/"))

    @contextlib.contextmanager
    def active(self):
        global _EXECUTION_CONTEXT
        old_execution_context = _EXECUTION_CONTEXT
        if old_execution_context is not self:  # slight optimiation for the common case
            _EXECUTION_CONTEXT = self.chroot() if self.sandbox else self
        try:
            yield
        finally:
            _EXECUTION_CONTEXT = old_execution_context


_EXECUTION_CONTEXT = ExecutionContext(ROOT)

# WARNING: THINK ABOUT THIS A LOT SOMETIME
# Potential for security holes here. For instance, if we can pass a callback
# to a service and get it to execute it, and that callback grabs execution context,
# we could leak or allow setting an execution context that's not ours.
def current_execution_context():
    return _EXECUTION_CONTEXT
