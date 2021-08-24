from typing import Coroutine

from .execution_context import ExecutionContext, current_execution_context
from .service import ServiceCall, ServiceService, TheServiceServiceBackend


# TODO: Errors should be nice, eg. "did you mean...?"
class Error(Exception):
    """Base service call error."""


class ServiceNotFound(Error):
    """Didn't find the service in the services lookup."""


class ServiceDidNotStart(Error):
    """There was a failure starting a backend for the service."""


class ServiceHadNoMatchingEndpoint(Error):
    """The service backend for that service didn't have the requested method endpoint."""


class InvalidExecutionContextRequested(Error):
    """The execution context requested in the service call asked for permissions it
    doesn't have."""


def validate_execution_context(ec: ExecutionContext, requested_ec: ExecutionContext):
    """Do security checks; if the requested execution context requests more permissions
    than the current one, reject it and raise an exception."""
    # For now just checking that we're not breaking chroot.
    abs_root = ec.root.resolve()
    requested_root = requested_ec.root.resolve()
    if not requested_root.is_relative_to(abs_root):
        raise InvalidExecutionContextRequested(
            f"New root was not a subset of the old root: {requested_ec.root}"
        )


async def handle_service_call(
    ec: ExecutionContext, services: ServiceService.Backend, service_call: ServiceCall
) -> any:
    """
    1. validate and set the execution context
    2. find the backend service
    3. schedule and await on the service call
    4. save result to service_call_result
    """
    requested_ec = service_call.execution_context
    validate_execution_context(ec, requested_ec)
    # we guarantee that the ServiceService will not make system calls
    backend = await services.get_backend(service_call.service, service_call.service_id)
    if not hasattr(backend, service_call.endpoint):
        raise ServiceHadNoMatchingEndpoint(service_call.service, service_call.endpoint)
    endpoint = getattr(backend, service_call.endpoint)
    with requested_ec.active():
        return await kernel_execute_coroutine(
            requested_ec,
            services,
            endpoint(*service_call.args, **service_call.kwargs),
        )


# TODO: I realized that rather than making this re-entrant for ServiceCalls, it makes
#       more sense to just implement a basic scheduler and schedule them.
#       The simplest scheduler will have a list of `coro`s instead of one and decide
#       which to call into. Next, we can track which ServiceCalls are being awaited
#       on by which processes, and only move them back to `active` when their service
#       calls return.
async def kernel_execute_coroutine(
    ec: ExecutionContext, services: ServiceService.Backend, coro: Coroutine
) -> any:
    """Orchestrates a program (main), allowing it to yield ServiceCall objects,
    which are then executed and the results sent back to the coroutine.
    Also manages execution context including validation and security checks,
    and manages looking up (and possibly starting) service backends.

    Make sure to use fine grained errors.
    Also, tracebacks are good! System should be developer friendly,
    err on the side of more info even if it exposes system internals.
    """
    # We need to decide whether to .send or .throw depending on whether there was an
    # error executing the service call. Sending exceptions via .throw allows programs
    # to handle service call exceptions normally and potentially recover.
    # On the first pass we send(None) to start the coroutine.
    service_call_result = None
    last_service_call_threw = False

    while True:
        # Execute the main program until it yields a ServiceCall object.
        # When send or throw raise StopIteration, the program has exited.
        try:
            if last_service_call_threw:
                # Right now throwing doesn't actually show the service traceback
                service_call = coro.throw(
                    type(service_call_result),
                    service_call_result.args,
                    service_call_result.__traceback__,
                )
            else:
                service_call = coro.send(service_call_result)
        except StopIteration as e:
            return e.value
        last_service_call_threw = False

        # The program made a service call.
        try:
            service_call_result = await handle_service_call(ec, services, service_call)
        except Exception as e:
            service_call_result = e
            last_service_call_threw = True


async def kernel_main(main: Coroutine):
    services = TheServiceServiceBackend()
    execution_context = current_execution_context()
    await kernel_execute_coroutine(execution_context, services, main)
