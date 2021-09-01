import asyncio
import dataclasses
import dill
import functools
from pathlib import Path
import re
import readline

from rich import print
from rich.console import Console
from rich.pretty import Pretty
from rich.prompt import Prompt
from rich.table import Table

from sos.execution_context import current_execution_context, User

from sos.services import Services
from sos.services.files import Files


LOCAL_ROOT = Path(".sos-hard-drive")


@dataclasses.dataclass
class Args:
    # I still don't have the user model figured out so I'm pretty sure
    # this _should not_ be an argument. Still tihs is basically a login shell right?
    user: User
    host: str = "localhost"
    port: int = 2222


# TODO: flatten nested schemas
def tabular(rows: list[dataclasses.dataclass]):
    if not rows:
        return r"\[\]"
    first = rows[0]
    if isinstance(first, tuple):
        table = Table(*([""] * len(first)), show_header=False, show_edge=False)
        for row in rows:
            table.add_row(*(Pretty(c) for c in row))
        return table
    elif dataclasses.is_dataclass(first):
        table = Table(
            *[f"{field.name}  ({field.type})" for field in dataclasses.fields(first)],
            show_header=True,
            show_edge=False,
        )
        for row in rows:
            table.add_row(*(Pretty(c) for c in dataclasses.astuple(row)))
        return table
    else:
        table = Table(show_header=False, show_edge=False)
        for row in rows:
            table.add_row(Pretty(row))
        return table


class Prompt(Prompt):
    prompt_suffix = ""

    @classmethod
    def repl(cls, user, path):
        user_s = f"[bright_yellow]{user.name}[/]"
        sys_s = "[bright_green]@[bright_cyan]sos-prototype"
        path_s = f"[white]([bright_magenta]{path}[/])"
        return cls.ask(f"{user_s}{sys_s} {path_s} [bright_white]>>> ")


class Shell:
    def __init__(self, args: Args):
        self.args = args
        self.ec = current_execution_context().replace(user=self.args.user)
        self.session_token = None

    def cd(self, directory: Path):
        self.ec = self.ec.replace(
            working_directory=(self.ec.full_path / directory).resolve(),
        )

    async def authorize(self):
        self.session_token = await self.remote_command("authorize", self.ec.user, None)

    async def remote_command(self, endpoint_name, *args):
        reader, writer = await asyncio.open_connection(self.args.host, self.args.port)
        writer.write(
            dill.dumps(
                (self.session_token, endpoint_name, args), byref=True, recurse=False
            )
        )
        writer.write_eof()
        response = dill.loads(await reader.read())
        if isinstance(response, Exception):
            raise response
        return response

    async def run(self, coro):
        return await self.remote_command("run", coro)

    async def service_call(self, service_name, endpoint_name, *args: str):
        service_name, service_id = re.match(
            r"^(\w+)(?:\((.*)\))?$", service_name
        ).groups()
        return await self.run(
            functools.partial(
                self._service_call, service_name, service_id, endpoint_name, args
            )
        )

    def print(self, result):
        if not isinstance(result, str) and hasattr(result, "__iter__"):
            print(tabular(result))
        else:
            print(repr(result))

    @staticmethod
    async def _service_call(service_name, service_id, endpoint_name, args):
        # All code executes remotely
        def _eval(s) -> any:
            try:
                return eval(s)
            except Exception:
                return s

        call_args = [_eval(arg) for arg in args]
        services = await Services().list_services()
        for service in services:
            if service.__name__ == service_name:
                service_handle = service(service_id)
                endpoint = getattr(service_handle, endpoint_name)
                return await endpoint(*call_args)
        else:
            raise Exception(f"No service found: {service_name}")

    async def main(self):
        await self.authorize()

        while (
            line := Prompt.repl(self.args.user, self.ec.working_directory)
        ) != "exit":
            # probably use the shell tools here :)
            args = line.strip().split()

            # and some smarter arg parsing / function registration here :)
            try:
                if args[0] == "cd":  # TODO
                    self.cd(Path(args[1]))
                if args[0] == "ls":
                    ls_args = [Path(a) for a in (args[1:] or ["."])]

                    files = await self.run(
                        functools.partial(Files().list_directory, ls_args[0])
                    )
                    # TODO: flatten nested schemas
                    self.print(sorted(files))
                if args[0] == "show":

                    async def show_file():
                        return await Files().read(Path(args[1])).value

                    print(await self.run(show_file).decode("utf-8"))
                if args[0:2] == ["list", "services"]:
                    self.print(await self.service_call("Services", "list_services"))
                if args[0:2] == ["list", "backends"]:
                    self.print(
                        await self.service_call("Services", "list_backends", args[2])
                    )
                if args[0] == "call":
                    self.print(await self.service_call(*args[1:]))
                if args[0] == "add_math":
                    # This _sort of_ works!!! Rather it definitely does work, but for instance
                    # > call Services list_backends Math
                    # returns an empty list which is untrue; I believe it does this because
                    # eval('Math') returns a different object than the actual Math service class
                    # > call Services list_backends Service
                    # > call Services list_services
                    # both fail with a recursion error. I've verified that this is _not_ because
                    # of ServiceMeta.SERVICES.

                    async def run_remote():
                        import dataclasses
                        from sos.services.logs import log

                        class Math(Service):
                            async def inc(self, x: int) -> int:
                                pass

                        class SimpleMathBackend(Math.Backend):
                            @dataclasses.dataclass
                            class Args:
                                inc_by: int = 5

                            async def inc(self, x: int) -> int:
                                await log(x=x, inc_by=self.args.inc_by)
                                return x + self.args.inc_by

                        await Services().register_backend(
                            Math, SimpleMathBackend, SimpleMathBackend.Args(4)
                        )

                    await self.run(run_remote)
                    print("Complete!")

            except Exception:
                Console().print_exception(show_locals=False)


if __name__ == "__main__":
    histfile = LOCAL_ROOT / ".history"
    try:
        readline.read_history_file(histfile)
    except Exception:
        pass
    try:
        asyncio.run(Shell(Args(User("stef"))).main())
    finally:
        readline.write_history_file(histfile)
