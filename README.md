# dimutex

Python library implementing [asyncio][asyncio]-based distributed mutex on top of different providers.

[Mutex][mutex] is a synchronization primitive used to ensure that only one worker can do the given job. It can be used for safe access to a shared resource or for distributing tasks among multiple workers.

Currently, the only implemented provider is GCS (Google Cloud Storage). The implementation is based on the algorithm described in article [A robust distributed locking algorithm based on Google Cloud Storage][gcs-algo] (see also [Ruby implementation][ruby]).

[asyncio]: https://docs.python.org/3/library/asyncio.html
[mutex]: https://stackoverflow.com/questions/34524/what-is-a-mutex
[gcs-algo]: https://www.joyfulbikeshedding.com/blog/2021-05-19-robust-distributed-locking-algorithm-based-on-google-cloud-storage.html
[ruby]: https://github.com/FooBarWidget/distributed-lock-google-cloud-storage-ruby

## Features

+ Asynchronous.
+ Type-safe.
+ Atomic.
+ Expiration mechanism to ensure that a single worker won't hold the lock forever.
+ Supports emulators.

## Installation

```bash
python3 -m pip install dimutex
```

## Usage

```python
import dimutex

async def do_something():
    lock = dimutex.GCS(bucket='bucket-name', name='lock-name')
    # context manager makes sure to close aiohttp session
    async with lock:
        try:
            await lock.acquire()
        except dimutex.AlreadyAcquiredError:
            return 'already acquired'
        try:
            # do something with the shared resource
            ...
            # update expiration if you need more time
            await lock.refresh()
            ...
        finally:
            await lock.release()
```
