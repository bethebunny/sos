from pathlib import Path
from file_service import File, Files2

import pytest


@pytest.mark.asyncio
async def test_service_simple():
    pointer_path = "/path-to-write"
    path = "/a/b/c"

    files = Files2()
    await files.write(pointer_path, File[str](path))
    pointer_contents = await files.read(pointer_path).value
    assert pointer_contents == path

    # don't do lazy eval; explicitly await on service call result
    path_to_write = await files.read(pointer_path).value
    await files.write(path_to_write, File[str]("we did it!"))
    result = await files.read(path).value
    assert result == "we did it!"


@pytest.mark.asyncio
async def test_getattr_lazy_service_results():
    pointer_path = "/path-to-write"
    path = "/a/b/c"

    files = Files2()
    await files.write(pointer_path, File[str](path))
    pointer_contents = await files.read(pointer_path).value
    assert pointer_contents == path

    # here we're exercising delayed execution of arguments, note no await on read
    path_to_write = files.read(pointer_path).value
    await files.write(path_to_write, File[str]("we did it!"))
    result = await files.read(path).value
    assert result == "we did it!"


@pytest.mark.asyncio
async def test_apply_lazy_service_results():
    pointer_path = "/path-to-write"
    path = "/a/b/c"

    files = Files2()
    await files.write(Path(pointer_path), File[str](path))
    pointer_contents = await files.read(Path(pointer_path)).value
    assert pointer_contents == path

    # here we're exercising delayed execution of arguments, note no await on read
    path_to_write = files.read(Path(pointer_path)).value
    # note that `path_to_write` points to a string, so we .apply on it to
    # change the promise type into a `Path`. Again this would work more easily
    # in Scala because we could type these operations better.
    await files.write(path_to_write.apply(Path), File[str]("we did it!"))
    result = await files.read(Path(path)).value
    assert result == "we did it!"


@pytest.mark.asyncio
async def test_apply_with_service_call():
    from functools import partial

    pointer_path = Path("/path-to-write")
    path = Path("/a/b/c")

    files = Files2()
    await files.write(pointer_path, File[Path](path))
    pointer_contents = await files.read(pointer_path).value
    assert pointer_contents == path

    expected_result = File[str]("we did it!")
    write_file = partial(files.write, file=expected_result)
    await files.read(pointer_path).value.apply(write_file)
    result = await files.read(path).value
    assert result == "we did it!"
