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


@pytest.mark.asyncio
async def test_lock_unlock(random_name: str, bucket: str):
    m = mutex.GCS(
        bucket=bucket,
        name=random_name,
    )
    await m.acquire()
    await m.release()
