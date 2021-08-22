import functools
from pathlib import Path
from file_service import File, Files2
from service import kernel_main

import pytest


# TODO: test fixture for Files2() that clears out the backend


@pytest.fixture
def files():
    Files2._backend = None
    return Files2()


def async_kernel_test(test_fn):
    @functools.wraps(test_fn)
    async def wrapped(*args, **kwargs):
        return await kernel_main(test_fn(*args, **kwargs))

    return pytest.mark.asyncio(wrapped)


@async_kernel_test
async def test_service_simple(files):
    async def test():
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

    await kernel_main(test())


@async_kernel_test
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


@async_kernel_test
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


@async_kernel_test
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


@async_kernel_test
async def test_shared_file_backend():
    pointer_path = Path("/path-to-write")
    path = Path("/a/b/c")

    # clear out backend
    Files2._backend = None

    await Files2().write(pointer_path, File[Path](path))
    pointer_contents = await Files2().read(pointer_path).value
    assert pointer_contents == path

    # don't do lazy eval; explicitly await on service call result
    path_to_write = await Files2().read(pointer_path).value
    await Files2().write(path_to_write, File[str]("we did it!"))
    result = await Files2().read(path).value
    assert result == "we did it!"


@async_kernel_test
async def test_execution_context_change_directory(files):
    await files.write(Path("/a/b/c"), File[str]("stuff"))
    with files.change_directory(Path("/a")):
        assert (await files.read(Path("b/c"))).value == "stuff"
        assert (await files.read(Path("/a/b/c"))).value == "stuff"


@async_kernel_test
async def test_execution_context_change_directory_relative_access(files):
    await files.write(Path("/a/b/c"), File[str]("stuff"))
    with files.change_directory(Path("/a/b/d")):
        assert (await files.read(Path("../c"))).value == "stuff"


@async_kernel_test
async def test_execution_context_change_root(files):
    await files.write(Path("/a/b/c"), File[str]("stuff"))
    assert (await files.read(Path("/a/b/c"))).value == "stuff"
    with files.change_root(Path("/a/b")):
        assert (await files.read(Path("/c"))).value == "stuff"


@async_kernel_test
async def test_execution_context_change_root_hides_original_root(files):
    await files.write(Path("/a/secret"), File[str]("stuff"))
    assert (await files.read(Path("/a/secret"))).value == "stuff"
    with files.change_root(Path("/a/b")):
        with pytest.raises(KeyError):
            await files.read(Path("/a/secret"))


@pytest.mark.asyncio
@async_kernel_test
async def test_execution_context_change_root_stops_relative_access(files):
    await files.write(Path("/a/secret"), File[str]("stuff"))
    assert (await files.read(Path("/a/secret"))).value == "stuff"
    with files.change_root(Path("/a/b")):
        with pytest.raises(ValueError):
            await files.read(Path("../secret"))
