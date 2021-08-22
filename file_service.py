import dataclasses
from pathlib import Path
import time
import typing

from execution_context import ExecutionContext, current_execution_context
from service import Service


T = typing.TypeVar("T")

# Files should have a strong record type
@dataclasses.dataclass
class File(typing.Generic[T]):
    # What if `read` and `write` are the wrong abstractions?
    # Should files be a typed, mutable object?!
    # what about FileHandle.{get,set,append,commit}?
    # Hmm. I think not. I think thinking about filesystems as a key-value
    # store makes more sense. Having file handles be able to "think" or "do"
    # too much... is that a good idea? It's really nice for .append
    value: T


class RawBinaryFile(File[bytes]):
    # This interface provides the "normal" filehandle API which allows random
    # file access, in addition to the higher-level File APIs.
    def seek(self, offset: int) -> None:
        pass

    async def read_up_to_n_bytes(self, n: int) -> bytes:
        pass

    # Do read/write modes still make sense?
    async def resize(self, size: int) -> None:
        pass

    async def write_bytes(self, bytes: bytes) -> bytes:
        pass


class SequentialFile(File[list[T]]):
    def append(self, T) -> None:
        pass


Timestamp = float
# TimeseriesFile = SequentialFile[(Timestamp, T)]


# I probably need my own record type that I can make more efficient than dataclass
# but that can come later
@dataclasses.dataclass
class FileMetadata(typing.Generic[T]):
    filetype: typing.Type[T]
    size: int
    last_modification_time: Timestamp
    creation_time: Timestamp

    # Security parameters need to be better thought out
    # permissions: TODO
    # owner: User  # property?
    # group: Group  # property?


# Right now this is basically a service stub.
# Figuring out service definitions and versioning is going to be key.
# Also, what are "path"s?
class Files(Service):
    async def stat(self, path: Path) -> FileMetadata:
        pass

    # What if `read` and `write` are the wrong abstractions?
    # Should files be a typed, mutable object?!
    # what about FileHandle.{get,set,append,commit}?
    async def read(self, path: Path) -> File:
        pass

    async def write(self, path: Path, file: File) -> None:
        pass

    async def append(self, path: Path, T):
        """Only applies to files of type SequentialFile."""
        pass


# I think the next thing I'd like to do is to stub out a real "Filesystem"
# service that actually works, and then make a simple shell that can interact
# with it and try do some basic things. In doing that I want to think at least
# a little bit about what the contract for service state is; my stub should keep
# a map somewhere in memory and maybe even saved as json, but who owns that memory?
class Files1(Files):
    async def stat(self, path: Path) -> FileMetadata:
        return FileMetadata(
            bytes,
            50,
            time.time() - 100,
            time.time() - 50,
        )

    async def read(self, path: Path) -> File:
        return self.stat(path).size


@dataclasses.dataclass
class FileRecord:
    metadata: FileMetadata
    file: File


class Files2(Files):
    # The Service definition class is both an interface/client and the implementation.
    # When you define a Service class, the metaclass creates the separate
    # implementations, and this becomes a thin client interface. When yielding
    # out to the service calls, the kernel code will be able to look at
    # the ExecutionContext for the yielding task and pass it to the "backend"
    # implementation. This backend is the one that's actually able to run
    # "priveleged" code.
    #
    # This probably isn't the best way to store state on the backend classes :)
    # So next I really need to figure out how Services get configured, so eg.
    # I can have an instance of this in the unit tests that doesn't just overwrite
    # files created by the shell xDDD
    _data: dict[Path, FileRecord] = {}

    @staticmethod
    def resolve_path(ec: ExecutionContext, path: Path) -> Path:
        # Hmm awkward. If you are switching chroots all of the time,
        # then paths are weird to keep track of. For instance if I output
        # a path name running in a sandbox, I want the user to _know_ that
        # it's a relative path to the working directory.
        if path.is_absolute():
            abspath = ec.root.joinpath(path.relative_to("/"))
        else:
            print(ec)
            abspath = ec.root.joinpath(ec.working_directory.relative_to("/")).joinpath(
                path
            )
        abspath = abspath.resolve()
        if not abspath.is_relative_to(ec.root):
            raise ValueError(f"path {path} referenced above root")
        return abspath

    async def stat(self, path: Path) -> FileMetadata:
        path = self.resolve_path(current_execution_context(), path)
        return self._data[path].metadata

    # I'm pretty sure if we're going through the trouble of having structured
    # files, which is _pretty sweet_, we can make the interfaces more typed
    # by default too; for instance maybe stat/read/write is the wrong interface,
    # and almost certainly whatever "read" is should return an object which is
    # more aware of the file metadata rather than just a byte stream.
    async def read(self, path: Path) -> File:
        path = self.resolve_path(current_execution_context(), path)
        return self._data[path].file

    async def write(self, path: Path, file: File) -> None:
        path = self.resolve_path(current_execution_context(), path)
        data = self._data
        record = FileRecord(
            metadata=FileMetadata(
                # Unsure where __orig_class__ comes from, possibly dataclass
                filetype=file.__orig_class__.__args__[0],
                size=0,
                creation_time=time.time(),
                last_modification_time=time.time(),
            ),
            file=file,
        )
        if path in data:
            old_record = data[path]
            record.metadata.creation_time = old_record.metadata.creation_time
        data[path] = record

    async def list_directory(self, path: Path) -> list[(Path, FileMetadata)]:
        path = self.resolve_path(current_execution_context(), path)
        return [
            (filepath, record.metadata)
            for filepath, record in self._data
            if path == filepath.parent
        ]

    def change_directory(self, path: Path):
        return dataclasses.replace(
            current_execution_context(),
            working_directory=self.resolve_path(current_execution_context(), path),
        ).active()

    def change_root(self, path: Path):
        # Hmm this won't set the execution context for self anyway :/
        return current_execution_context().chroot(path).active()


class Window:
    @property
    def width(self) -> int:
        pass

    @property
    def height(self) -> int:
        pass

    async def draw(self, framebuffer: bytes):
        pass
