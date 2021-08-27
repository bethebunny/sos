import contextlib
import dataclasses
from pathlib import Path


@dataclasses.dataclass
class User:
    name: str


ROOT = User("root")


# TODO:
#   - think more carefully about user model and what users are/mean
#   - let ExecutionContext be used as a context manager (rather than .active())
#   - current_execution_context() is really wordy and we use it everywhere
#   - try to implement using contextvars instead of a module global
#       ... or understand why that's not a good idea :)


@dataclasses.dataclass(frozen=True)
class ExecutionContext:
    """The core ExecutionContext representing the permissions that a given scope
    of code has. Generally we'd like the security of the kernel design to meet as
    many of these criteria as possible.

        1 practical: people won't use systems that are cumbersome or have high mental load
        2 easy: things are more secure when the easiest way to do them is the secure way
        3 isolation: "unsafe" operations should have as small a surface area as possible
        4 least privilege: run things in the lowest privilege mode they need
        5 write up, read down: data can only flow in the direction of higher privilege

    Roughly this is ordered by importance where ideologies conflict. Don't sacrifice
    practicality for perfection; make something useful and fun, but when compromises are
    made, keep track of them and aspire to a future system with fewer compromises.
    """

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
        if old_execution_context is not self:  # slight optimization for the common case
            _EXECUTION_CONTEXT = self.chroot() if self.sandbox else self
        try:
            yield
        finally:
            _EXECUTION_CONTEXT = old_execution_context


_EXECUTION_CONTEXT = ExecutionContext(ROOT)


def current_execution_context():
    return _EXECUTION_CONTEXT
