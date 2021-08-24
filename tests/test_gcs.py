from random import choice
from string import ascii_letters
import pytest
import os

import mutex


@pytest.fixture
def bucket() -> str:
    return os.environ['BUCKET']


@pytest.fixture
def random_name() -> str:
    return ''.join(choice(ascii_letters) for _ in range(20))


@pytest.fixture
async def lock(random_name: str, bucket: str):
    m = mutex.GCS(
        bucket=bucket,
        name=random_name,
    )
    async with m:
        yield m


@pytest.mark.asyncio
async def test_lock_unlock(lock: mutex.GCS):
    await lock.acquire()
    await lock.release()


@pytest.mark.asyncio
async def test_cannot_lock_twice(lock: mutex.GCS):
    await lock.acquire()
    with pytest.raises(mutex.AlreadyAcquiredError):
        await lock.acquire()
    await lock.release()


@pytest.mark.asyncio
async def test_cannot_unlock_twice(lock: mutex.GCS):
    await lock.acquire()
    await lock.release()
    with pytest.raises(mutex.AlreadyReleasedError):
        await lock.release()
