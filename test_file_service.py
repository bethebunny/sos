import asyncio
from execution_context import current_execution_context
import functools
import inspect
from pathlib import Path
from service import ServiceService
from file_service import File, Files, InMemoryFilesystem
from kernel_main import kernel_main

import pytest


@pytest.fixture
async def files():
    await ServiceService().register_backend(
        Files,
        InMemoryFilesystem,
        InMemoryFilesystem.Args(),
    )
    yield Files()


@pytest.mark.kernel
async def test_service_simple(files):
    pointer_path = Path("/path-to-write")
    path = Path("/a/b/c")

    await files.write(pointer_path, File[Path](path))
    pointer_contents = await files.read(pointer_path).value
    assert pointer_contents == path

    # don't do lazy eval; explicitly await on service call result
    path_to_write = await files.read(pointer_path).value
    await files.write(path_to_write, File[str]("we did it!"))
    result = await files.read(path).value
    assert result == "we did it!"


@pytest.mark.kernel
async def test_getattr_lazy_service_results(files):
    pointer_path = Path("/path-to-write")
    path = Path("/a/b/c")

    await files.write(pointer_path, File[Path](path))
    pointer_contents = await files.read(pointer_path).value
    assert pointer_contents == path

    # here we're exercising delayed execution of arguments, note no await on read
    path_to_write = files.read(pointer_path).value
    await files.write(path_to_write, File[str]("we did it!"))
    result = await files.read(path).value
    assert result == "we did it!"


@pytest.mark.kernel
async def test_apply_lazy_service_results(files):
    pointer_path = Path("/path-to-write")
    path = Path("/a/b/c")

    await files.write(pointer_path, File[str](str(path)))
    pointer_contents = await files.read(pointer_path).value
    assert pointer_contents == str(path)

    # here we're exercising delayed execution of arguments, note no await on read
    path_to_write = files.read(pointer_path).value
    # note that `path_to_write` points to a string, so we .apply on it to
    # change the promise type into a `Path`. Again this would work more easily
    # in Scala because we could type these operations better.
    await files.write(path_to_write.apply(Path), File[str]("we did it!"))
    result = await files.read(path).value
    assert result == "we did it!"


@pytest.mark.kernel
async def test_apply_with_service_call(files):
    from functools import partial

    pointer_path = Path("/path-to-write")
    path = Path("/a/b/c")

    await files.write(pointer_path, File[Path](path))
    pointer_contents = await files.read(pointer_path).value
    assert pointer_contents == path

    expected_result = File[str]("we did it!")
    write_file = partial(files.write, file=expected_result)
    await files.read(pointer_path).value.apply(write_file)
    result = await files.read(path).value
    assert result == "we did it!"


@pytest.mark.kernel
async def test_shared_file_backend(files):
    pointer_path = Path("/path-to-write")
    path = Path("/a/b/c")

    # not using the files handle, creating a new one each time
    await Files().write(pointer_path, File[Path](path))
    pointer_contents = await Files().read(pointer_path).value
    assert pointer_contents == path

    # don't do lazy eval; explicitly await on service call result
    path_to_write = await Files().read(pointer_path).value
    await Files().write(path_to_write, File[str]("we did it!"))
    result = await Files().read(path).value
    assert result == "we did it!"


@pytest.mark.kernel
async def test_execution_context_change_directory(files):
    await files.write(Path("/a/b/c"), File[str]("stuff"))
    with current_execution_context().full().active():
        with files.change_directory(Path("/a")):
            assert (await files.read(Path("b/c"))).value == "stuff"
            assert (await files.read(Path("/a/b/c"))).value == "stuff"


@pytest.mark.kernel
async def test_execution_context_change_directory_relative_access(files):
    await files.write(Path("/a/b/c"), File[str]("stuff"))
    with current_execution_context().full().active():
        with files.change_directory(Path("/a/b/d")):
            assert (await files.read(Path("../c"))).value == "stuff"


@pytest.mark.kernel
async def test_execution_context_change_directory_sandbox(files):
    await files.write(Path("/a/b/c"), File[str]("stuff"))
    with files.change_directory(Path("/a")):
        assert (await files.read(Path("b/c"))).value == "stuff"
        assert (await files.read(Path("/b/c"))).value == "stuff"
        with pytest.raises(KeyError):
            await files.read(Path("/a/b/c"))


@pytest.mark.kernel
async def test_execution_context_change_root(files):
    await files.write(Path("/a/b/c"), File[str]("stuff"))
    assert (await files.read(Path("/a/b/c"))).value == "stuff"
    with files.change_root(Path("/a/b")):
        assert (await files.read(Path("/c"))).value == "stuff"


@pytest.mark.kernel
async def test_execution_context_change_root_hides_original_root(files):
    await files.write(Path("/a/secret"), File[str]("stuff"))
    assert (await files.read(Path("/a/secret"))).value == "stuff"
    with files.change_root(Path("/a/b")):
        with pytest.raises(KeyError):
            await files.read(Path("/a/secret"))


@pytest.mark.kernel
async def test_execution_context_change_root_stops_relative_access(files):
    await files.write(Path("/a/secret"), File[str]("stuff"))
    assert (await files.read(Path("/a/secret"))).value == "stuff"
    with files.change_root(Path("/a/b")):
        with pytest.raises(ValueError):
            await files.read(Path("../secret"))
