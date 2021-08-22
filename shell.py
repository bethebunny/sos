import dataclasses
from pathlib import Path

# hmmm probably not quite right but this is how I'll do it for now :)

from execution_context import current_execution_context, User
from kernel_main import kernel_main

from file_service import Files2

# This should be standard and adapted for services also :)
@dataclasses.dataclass
class Args:
    # I still don't have the user model figured out so I'm pretty sure
    # this _should not_ be an argument.
    user: User


class Shell:
    def __init__(self, args: Args):
        self.args = args
        self.ec = dataclasses.replace(
            current_execution_context(),
            user=self.args.user,
        )

    def resolve_path(self, path: Path) -> Path:
        if path.is_absolute():
            return path.resolve()
        return self.ec.working_directory.joinpath(path).resolve()

    def cd(self, directory: Path):
        self.ec = dataclasses.replace(
            self.ec, working_directory=self.resolve_path(directory)
        )

    @property
    def prompt(self) -> str:
        return f"{self.args.user.name}@sos-prototype [{self.ec.working_directory}] >>> "

    # Should programs _also_ be services? Probably not?
    async def main(self):
        while (line := input(self.prompt)) != "exit":
            # probably use the shell tools here :)
            args = line.strip().split()

            # some kind of map or service type
            if args[0] == "cd":
                if len(args) != 2:
                    print(f"invalid number of args to cd")
                    continue
                self.cd(Path(args[1]))
            if args[0] == "ls":
                ls_args = [Path(a) for a in (args[1:] or ["."])]
                if len(ls_args) > 1:
                    print(f"Invalid # of LS args")
                    continue
                with self.ec.active():
                    files = await Files2().list_directory(self.resolve_path(ls_args[0]))
                for filepath, metadata in sorted(files):
                    # TODO: maybe use rich or something? :)
                    print(f"{filepath} -> {metadata.filetype}")


import asyncio

loop = asyncio.get_event_loop()
loop.run_until_complete(kernel_main(Shell(Args(User("stef"))).main()))
