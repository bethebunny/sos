# sos
A python coroutine-based OS prototype based on strongly typed Services as system components.

The core ideas of the system are that
- The one system call is the Service call
- Services and their methods are strongly typed
- All Service methods and scheduled code (eg. `main`) are `async` functions
- The system doesn't really care where a service is running -- even core services could be remote!
- Service functions can [time travel](https://capnproto.org/rpc.html)!
  - You can execute deferred computations -- including other service calls! -- on the results
    of a service call before it's awaited. When you await on it, calls may be batched to eliminate
    some or many trips to remote services!

More implementation details
- Service implementations can be viewed, registered, and swapped out via the `Services` service
- `await schedule(...)` and `await gather(...)` for simple and powerful concurrency

## Installation

I recommend normal python venv installation. Requires python >= 3.9.
```
mkdir ~/.venvs
python3.9 -m venv ~/.venvs/sos
source ~/.venvs/sos/bin/activate
pip install -r requirements-dev.txt
```

## Shell

```
python shell.py
```
will start a simple login shell environment in tho OS. Try a couple basic commands like `list services` or `list backends Files`.

There's really not a lot here yet :)

## Unit tests

```
pytest .
```

