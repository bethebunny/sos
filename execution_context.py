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
# with current_execution_context().replace(user=User("stef"), root="/home/stef"):
#     files = Files()
@dataclasses.dataclass(frozen=True)
class ExecutionContext:
    user: User
    root: Path
    working_directory: Path
    # if sandbox, then by default activating will chroot
    sandbox: bool = True

    def full(self) -> "ExecutionContext":
        """An execution context which won't sandbox."""
        return dataclasses.replace(self, sandbox=False)

    def chroot(self, new_root: Optional[Path] = None) -> "ExecutionContext":
        # change root to new_root or working_directory
        if new_root is None:
            new_root = self.working_directory
        if new_root.is_absolute():
            new_root = new_root.relative_to("/")
        return dataclasses.replace(
            self,
            root=self.root.joinpath(new_root),
            working_directory=Path("/"),
        )

    @contextlib.contextmanager
    def active(self):
        print(f"Activating {self}")
        global _EXECUTION_CONTEXT
        old_execution_context = _EXECUTION_CONTEXT
        _EXECUTION_CONTEXT = self.chroot() if self.sandbox else self
        try:
            yield
        finally:
            _EXECUTION_CONTEXT = old_execution_context


_EXECUTION_CONTEXT = ExecutionContext(ROOT, Path("/"), Path("/"))

# WARNING: THINK ABOUT THIS A LOT SOMETIME
# Potential for security holes here. For instance, if we can pass a callback
# to a service and get it to execute it, and that callback grabs execution context,
# we could leak or allow setting an execution context that's not ours.
def current_execution_context():
    return _EXECUTION_CONTEXT
