import asyncio
import functools
import pickle
import cachetools


def _generate_key_from_partial(partial_obj):
    data = (
        partial_obj.func.__module__ + "." + partial_obj.func.__qualname__,
    ) + partial_obj.args

    for item in partial_obj.keywords.items():
        data += item

    if len(data) == 1:
        return data[0]

    return pickle.dumps(data)


def _generate_key_from_args(arg_obj):
    args, kwargs = arg_obj
    data = args
    for item in kwargs.items():
        data += item
    if len(data) == 1 and type(data[0]) in {int, str}:
        return data[0]
    return pickle.dumps(data)


class AsyncCache:
    cache_hits = 0
    cache_misses = 0

    def __init__(self, cache_backend=None):
        if cache_backend is None:
            cache_backend = cachetools.LRUCache(maxsize=128)

        self.cache_backend = cache_backend
        self.upd_futures = dict()

    async def get(self, key, default=None):
        if key in self.cache_backend:
            self.cache_hits += 1
            return self.cache_backend[key]
        elif key in self.upd_futures:
            self.cache_hits += 1
            return await self._wait_update(key)
        else:
            self.cache_misses += 1
            return default

    async def _wait_update(self, key):
        if key in self.upd_futures:
            fut = self.upd_futures[key]
            value = await fut
            self.cache_backend[key] = value
            return value
        return None

    def acquire_update(self, key):
        if key in self.upd_futures:
            return None

        self._create_update_future(key)
        return key

    def _create_update_future(self, key):
        loop = asyncio.get_running_loop()
        self.upd_futures[key] = loop.create_future()

    def release_update(self, key, value):
        self.cache_backend[key] = value
        self.upd_futures[key].set_result(value)

    async def offer(self, key, coro_fn):
        if key in self.upd_futures:
            return await self.get(key)
        else:
            self._create_update_future(key)
            res = await coro_fn()
            self.release_update(key, res)
            return res

    def cache_info(self):
        return {
            "hits": self.cache_hits,
            "misses": self.cache_misses,
            "cursize": len(self.cache_backend),
            "maxsize": self.cache_backend.maxsize,
        }


class AsyncCachedRunnner:
    cache_hits = 0
    cache_misses = 0

    def __init__(self, cache_backend_factory=None):
        if cache_backend_factory is None:
            self.cache_backend = cachetools.LRUCache(maxsize=128)
        else:
            self.cache_backend = cache_backend_factory()
        self.in_progress = dict()

    async def _run_and_update(self, closed_coro_fn, coro_fn_key):
        print(closed_coro_fn, coro_fn_key)
        self.cache_backend[coro_fn_key] = await closed_coro_fn()
        del self.in_progress[coro_fn_key]

    async def run(self, coro_fn, *args, **kwargs):
        closed_coro_fn = functools.partial(coro_fn, *args, **kwargs)
        coro_fn_key = _generate_key_from_partial(closed_coro_fn)

        if coro_fn_key in self.cache_backend:
            self.cache_hits += 1
            return self.cache_backend[coro_fn_key]

        if coro_fn_key in self.in_progress:
            self.cache_hits += 1
            await self.in_progress[coro_fn_key]
            return self.cache_backend[coro_fn_key]

        self.cache_misses += 1
        task = asyncio.create_task(self._run_and_update(closed_coro_fn, coro_fn_key))
        self.in_progress[coro_fn_key] = task
        await task
        return self.cache_backend[coro_fn_key]

    async def shutdown(self):
        await asyncio.wait(self.in_progress, timeout=60)

    def cache_info(self):
        return {
            "hits": self.cache_hits,
            "misses": self.cache_misses,
            "cursize": len(self.cache_backend),
            "maxsize": self.cache_backend.maxsize,
        }


def create_from_sync_cache(sync_cache) -> AsyncCache:
    return AsyncCache(cache_backend=sync_cache)


def asynccached(_coro=None, *, async_cache=None, key_fn=_generate_key_from_args):
    if async_cache is None:
        async_cache = AsyncCache()

    def _decorator(_coro):
        @functools.wraps(_coro)
        async def _wrapper(*args, **kwargs):
            key = key_fn((args, kwargs))
            res = await async_cache.get(key)
            if res is None:
                closed_coro_fn = functools.partial(_coro, *args, **kwargs)
                res = await async_cache.offer(key, closed_coro_fn)
            return res

        _wrapper.cache = async_cache
        _wrapper.cache_info = async_cache.cache_info
        return _wrapper

    if _coro is None:
        return _decorator
    else:
        return _decorator(_coro)
