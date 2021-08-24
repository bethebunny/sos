import dataclasses
from pathlib import Path

from rich import print
from rich.console import Console
from rich.pretty import Pretty
from rich.prompt import Prompt
from rich.table import Table
import readline

from sos.execution_context import current_execution_context, User
from sos.kernel_main import kernel_main

from sos.services import Services
from sos.services.files import Files, ProxyFilesystem


@dataclasses.dataclass
class Args:
    # I still don't have the user model figured out so I'm pretty sure
    # this _should not_ be an argument. Still tihs is basically a login shell right?
    user: User


def tabular(l: list[dataclasses.dataclass]):
    if not l:
        return r"\[\]"
    first = l[0]
    if isinstance(first, tuple):
        table = Table(*([""] * len(first)), show_header=False, show_edge=False)
        for row in l:
            table.add_row(*(Pretty(c) for c in row))
        return table
    elif dataclasses.is_dataclass(first):
        table = Table(
            *[f"{field.name}  ({field.type})" for field in dataclasses.fields(first)],
            show_header=True,
            show_edge=False,
        )
        for row in l:
            table.add_row(*(Pretty(c) for c in dataclasses.astuple(row)))
        return table
    else:
        table = Table(show_header=False, show_edge=False)
        for row in l:
            table.add_row(Pretty(row))
        return table


class Prompt(Prompt):
    prompt_suffix = ""

    @classmethod
    def repl(cls, user, path):
        user_s = f"[bright_yellow]{user.name}[/]"
        sys_s = f"[bright_green]@[bright_cyan]sos-prototype"
        path_s = f"[white]([bright_magenta]{path}[/])"
        return cls.ask(f"{user_s}{sys_s} {path_s} [bright_white]>>> ")


class Shell:
    def __init__(self, args: Args):
        self.args = args
        self.ec = current_execution_context().replace(user=self.args.user)

    def cd(self, directory: Path):
        self.ec = self.ec.replace(
            working_directory=(self.ec.full_path / directory).resolve(),
        )

    async def main(self):
        await Services().register_backend(
            Files,
            ProxyFilesystem,
            ProxyFilesystem.Args(local_root=Path(".sos-hard-drive")),
        )
        while (
            line := Prompt.repl(self.args.user, self.ec.working_directory)
        ) != "exit":
            # probably use the shell tools here :)
            args = line.strip().split()

            # and some smarter arg parsing / function registration here :)
            try:
                if args[0] == "cd":
                    self.cd(Path(args[1]))
                if args[0] == "ls":
                    ls_args = [Path(a) for a in (args[1:] or ["."])]
                    with self.ec.nosandbox().active():
                        files = await Files().list_directory(ls_args[0])
                    # TODO: flatten nested schemas
                    print(tabular(sorted(files)))
                if args[0] == "show":
                    with self.ec.nosandbox().active():
                        data = await Files().read(Path(args[1])).value
                        assert type(data) is bytes
                        print(data.decode("utf8"))
                if args[0:2] == ["list", "services"]:
                    with self.ec.active():
                        print(tabular(await Services().list_services()))
                if args[0:2] == ["list", "backends"]:
                    with self.ec.active():
                        services = await Services().list_services()
                        for service in services:
                            if service.__name__ == args[2]:
                                backends = await Services().list_backends(service)
                                print(tabular(backends))
            except Exception as e:
                Console().print_exception(show_locals=False)


if __name__ == "__main__":
    import asyncio

    loop = asyncio.get_event_loop()
    loop.run_until_complete(kernel_main(Shell(Args(User("stef"))).main()))
