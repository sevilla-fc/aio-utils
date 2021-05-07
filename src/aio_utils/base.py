import asyncio
import typing as _ty

import aiohttp
import tenacity
import logging
import functools

from .http import HttpRequest


def _apply_post_callback(request_coro, callback, loop):
    @functools.wraps(request_coro)
    async def _wrapped_request_coro(*args, **kwargs):
        response = await request_coro(*args, **kwargs)
        return await loop.run_in_executor(None, callback, response)

    return _wrapped_request_coro


async def _base_request_performer(req: HttpRequest, session, proxy, proxy_auth):
    async with session.request(
        req.method.value,
        req.url,
        json=req.json,
        params=req.params,
        headers=req.headers,
        proxy=proxy,
        proxy_auth=proxy_auth,
    ) as resp:
        st = resp.status
        response_data = await resp.json()
        return st, response_data


def retry_on_failure(coro, max_tries=3, delay=60, max_delay=600):
    logger = logging.getLogger(__name__)

    _retry_wrapper = tenacity.retry(
        wait=tenacity.wait_exponential(multiplier=delay, max=max_delay),
        stop=tenacity.stop_after_attempt(max_tries),
        before_sleep=tenacity.before_sleep_log(logger, logging.DEBUG),
    )(coro)

    @functools.wraps(_retry_wrapper)
    async def _wrapper(*args, **kwargs):
        try:
            return await _retry_wrapper(*args, **kwargs)
        except tenacity.RetryError as err:
            exc = err.last_attempt.exception()
            logger.error(f"Failed after {max_tries} tries. Got {exc!r}", exc_info=exc)
            raise exc from err

    return _wrapper


def retry_on_status(coro, max_attempts=100, max_delay=300, exp_mult=1, exp_max=60):
    logger = logging.getLogger(__name__)

    @tenacity.retry(
        wait=tenacity.wait_random_exponential(multiplier=exp_mult, max=exp_max),
        stop=tenacity.stop_after_delay(max_delay)
        | tenacity.stop_after_attempt(max_attempts),
        before_sleep=tenacity.before_sleep_log(logger, logging.DEBUG),
    )
    @functools.wraps(coro)
    async def _wrapper(*args, **kwargs):
        st, response = await coro(*args, **kwargs)
        if st == 200:
            return st, response
        else:
            print(st, response, *args)
            raise tenacity.TryAgain(
                f"TryAgain[status={st}, response={response}, "
                f"args={args}, kwargs={kwargs}]"
            )

    return _wrapper


def create_simple_requester(session, proxy=None, proxy_auth=None):
    partial_func = functools.partial(
        _base_request_performer, session=session, proxy=proxy, proxy_auth=proxy_auth
    )
    functools.update_wrapper(partial_func, _base_request_performer)
    return partial_func


def create_requester(session, proxy=None, proxy_auth=None):
    coro = create_simple_requester(session, proxy, proxy_auth)
    coro = retry_on_status(coro)
    return coro


async def perform_async_http_requests(
    requests: _ty.Sequence[HttpRequest],
    response_callback: _ty.Optional[_ty.Callable[[_ty.Any], _ty.Any]] = None,
    concurrency: int = 1,
    proxy=None,
    proxy_auth=None,
) -> _ty.Sequence[_ty.Any]:
    conn = aiohttp.TCPConnector(limit=0)
    session = aiohttp.ClientSession(connector=conn)
    sem = asyncio.BoundedSemaphore(concurrency)
    results = [None] * len(requests)

    requester_coro = create_requester(session, proxy, proxy_auth)
    #requester_coro = create_simple_requester(session, proxy, proxy_auth)

    async def _request_http(request):
        async with sem:
            st, raw_response = await requester_coro(request)
            return raw_response

    # if there is any callback, wrap the request function with a post-request
    # callback applycation over the response data
    if response_callback:
        loop = asyncio.get_running_loop()
        _aux_request_coro = _apply_post_callback(_request_http, response_callback, loop)
    else:
        _aux_request_coro = _request_http

    async def _request_task(t_id, request):
        response = await _aux_request_coro(request)
        results[t_id] = response

    tasks = [
        asyncio.create_task(_request_task(i, req))
        for i, req in enumerate(requests)
        if req
    ]

    await asyncio.gather(*tasks)

    await session.close()
    return results


def create_executor_helper(executor=None):
    loop = asyncio.get_running_loop()

    def _run_in_exec(f, *args, **kwargs):
        closed_fun = functools.partial(f, *args, **kwargs)
        return loop.run_in_executor(executor, closed_fun)

    return _run_in_exec
