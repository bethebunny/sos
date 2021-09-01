"""
Run with
    python -m cProfile -s cumtime profile_service_calls.py | head -50
"""
import asyncio
import dataclasses

from sos.service import Service
from sos.services import Services
from sos.kernel_main import kernel_main


class A(Service):
    async def inc(self, x: int) -> int:
        pass

    async def triangle(self, x: int) -> int:
        pass


class SimpleA(A.Backend):
    async def inc(self, x: int) -> int:
        return x + 1

    async def triangle(self, x: int) -> int:
        return (x * (x + 1)) // 2


class OutsourceA(A.Backend):
    @dataclasses.dataclass
    class Args:
        outsource_id: str

    async def inc(self, x: int) -> int:
        return await A(self.args.outsource_id).inc(x)

    async def triangle(self, x: int) -> int:
        if x > 0:
            return x + await A(self.args.outsource_id).triangle(x - 1)
        return x


async def main():
    await Services().register_backend(
        A,
        OutsourceA,
        OutsourceA.Args(None),
    )
    await A().triangle(100000)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(kernel_main(main()))
