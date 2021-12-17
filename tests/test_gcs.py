import os
from datetime import datetime, timedelta
from random import choice
from string import ascii_letters

import pytest

import dimutex


@pytest.fixture
def bucket() -> str:
    return os.environ['BUCKET']


@pytest.fixture
def random_name() -> str:
    return ''.join(choice(ascii_letters) for _ in range(20))


@pytest.fixture
async def lock(random_name: str, bucket: str):
    m = dimutex.GCS(
        bucket=bucket,
        name=random_name,
    )
    async with m:
        yield m


@pytest.mark.asyncio
async def test_lock_unlock(lock: dimutex.GCS):
    await lock.acquire()
    await lock.release()


@pytest.mark.asyncio
async def test_lock_unlock__subdirectory(lock: dimutex.GCS):
    lock.name += '/subpath.bin'
    await lock.acquire()
    await lock.release()


@pytest.mark.asyncio
async def test_acquired_check(lock: dimutex.GCS):
    assert await lock.acquired() is False
    await lock.acquire()
    assert await lock.acquired() is True
    await lock.release()
    assert await lock.acquired() is False


@pytest.mark.asyncio
async def test_cannot_lock_twice(lock: dimutex.GCS):
    await lock.acquire()
    with pytest.raises(dimutex.AlreadyAcquiredError):
        await lock.acquire()
    await lock.release()


@pytest.mark.asyncio
async def test_cannot_unlock_twice(lock: dimutex.GCS):
    await lock.acquire()
    await lock.release()
    with pytest.raises(dimutex.AlreadyReleasedError):
        await lock.release()


@pytest.mark.asyncio
async def test_lock_twice_if_forced(lock: dimutex.GCS):
    await lock.acquire()
    await lock.acquire(force=True)
    await lock.release()


@pytest.mark.asyncio
async def test_lock_expired(lock: dimutex.GCS):
    now = datetime(2010, 11, 12, 13, 14, 15)
    lock.now = lambda: now  # type: ignore
    await lock.acquire()
    lock.now = lambda: now + timedelta(seconds=61)  # type: ignore
    await lock.acquire()
    await lock.release()


@pytest.mark.asyncio
async def test_dont_lock_expired_if_expired_is_false(lock: dimutex.GCS):
    now = datetime(2010, 11, 12, 13, 14, 15)
    lock.now = lambda: now  # type: ignore
    await lock.acquire()
    lock.now = lambda: now + timedelta(seconds=61)  # type: ignore
    with pytest.raises(dimutex.AlreadyAcquiredError):
        await lock.acquire(expired=False)
    await lock.release()


@pytest.mark.asyncio
async def test_lock_expired_race(lock: dimutex.GCS):
    """
    Even if there is no lock when we call `_release_expired`,
    we still should acquire the lock. It is possible if the lock
    is released after we checked it in `acquire` but before
    `_release_expired` is called.
    """
    await lock._release_expired()
    await lock.acquire()
    with pytest.raises(dimutex.AlreadyAcquiredError):
        await lock._release_expired()
    await lock.release()


@pytest.mark.asyncio
async def test_required_but_unused(lock: dimutex.GCS):
    with pytest.raises(AssertionError):
        async with lock:
            pass
    assert await lock.acquired() is False
