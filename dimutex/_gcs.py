import json
import os
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from typing import Callable, Dict, Optional
from urllib.parse import quote_plus

import aiohttp
from gcloud.aio.auth import Token

from ._exceptions import AlreadyAcquiredError, AlreadyReleasedError


def _get_emulator_url() -> Optional[str]:
    host = os.environ.get('STORAGE_EMULATOR_HOST')
    if not host:
        return None
    return f'https://{host}'


DEFAULT_URL = 'https://www.googleapis.com'
DEFAULT_TTL = timedelta(seconds=60)
EMULATOR_URL = _get_emulator_url()
SCOPES = [
    'https://www.googleapis.com/auth/devstorage.read_write',
]
BOUNDARY = 'cf58b63b6ce6f37881e9740f24be22d7'
TIME_FORMAT = '%Y-%d-%m %H:%M:%S.%f %Z'


class GCS:
    """

    Args:
        bucket:     GCS bucket name.
        name:       Lock name, used as filename of lock in GCS.
        api_url:    URL of GCS API, helpful for testing with emulator.
        now:        Callback used to determine the current time.
        ttl:        How long to wait before the lock considered to be stale.
        body:       File content for lock, may be useful for debugging.
        required:   If True, `acquire` must be called at least once.
    """
    __slots__ = [
        'bucket',
        'name',
        'api_url',
        'session',
        'token',
        'emulator',
        'ttl',
        'now',
        'body',
        'required',
    ]

    bucket: str
    name: str
    api_url: str
    session: aiohttp.ClientSession
    token: Token
    ttl: timedelta
    now: Callable[..., datetime]
    body: str
    required: bool

    def __init__(
        self,
        bucket: str,
        name: str,
        api_url: Optional[str] = None,
        session: Optional[aiohttp.ClientSession] = None,
        token: Optional[Token] = None,
        now: Callable[[], datetime] = datetime.now,
        ttl: timedelta = DEFAULT_TTL,
        body: str = 'lock',
        required: bool = True,
    ) -> None:
        self.bucket = bucket
        self.name = name
        self.emulator = api_url is not None or EMULATOR_URL is not None
        self.api_url = api_url or EMULATOR_URL or DEFAULT_URL
        self.ttl = ttl
        self.now = now  # type: ignore
        self.body = body
        self.required = required

        if session is None:
            session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=not self.emulator),
                timeout=aiohttp.ClientTimeout(total=10),
            )
        self.session = session
        if token is None:
            token = Token(scopes=SCOPES, session=session)  # type: ignore[arg-type]
        self.token = token

    async def _headers(self) -> Dict[str, str]:
        if self.emulator:
            return {}
        token = await self.token.get()
        return {
            'Authorization': f'Bearer {token}',
        }

    async def acquire(self, force: bool = False, expired: bool = True) -> None:
        """Acquire (lock) the mutex.

        Args:
            force: acquire even if it is already locked
            expired: acquire if the lock is expired

        Raises:
            AlreadyAcquiredError
            ClientResponseError
        """
        self.required = False
        resp = await self._create(force=force)
        if resp.status == HTTPStatus.PRECONDITION_FAILED:
            if not expired:
                raise AlreadyAcquiredError
            await self._release_expired()
            await self.acquire(force=force, expired=False)
            return
        resp.raise_for_status()

    async def release(self) -> None:
        """Release (unlock) the mutex

        Raises:
            AlreadyReleasedError
            ClientResponseError
        """
        self.required = False
        resp = await self._delete()
        if resp.status == HTTPStatus.NOT_FOUND:
            raise AlreadyReleasedError(self.name)
        resp.raise_for_status()

    async def refresh(self) -> None:
        """Refresh the mutex.

        The method postpones the mutex expiration to TTL.

        Raises:
            AlreadyReleasedError
            ClientResponseError
        """
        self.required = False
        resp = await self._patch()
        if resp.status == HTTPStatus.NOT_FOUND:
            raise AlreadyReleasedError(self.name)
        resp.raise_for_status()

    async def _release_expired(self) -> None:
        """Release the lock if and only if the lock exists but expired.

        Raises:
            ClientResponseError
            AlreadyAcquiredError
        """
        resp = await self._get()
        if resp.status == HTTPStatus.NOT_FOUND:
            return
        resp.raise_for_status()
        content = await resp.json()
        expires = datetime.strptime(content['metadata']['expires'], TIME_FORMAT)
        expires = expires.replace(tzinfo=timezone.utc)
        now = self.now().astimezone(timezone.utc)
        if now < expires:
            raise AlreadyAcquiredError

        resp = await self._delete(generation=content['generation'])
        resp.raise_for_status()

    async def acquired(self) -> bool:
        """Check is the mutex is already acquired (locked).

        Raises:
            ClientResponseError
        """
        self.required = False
        resp = await self._get()
        if resp.status == HTTPStatus.NOT_FOUND:
            return False
        resp.raise_for_status()
        return True

    def _make_expired(self) -> str:
        now = self.now().astimezone(timezone.utc)
        return (now + self.ttl).strftime(TIME_FORMAT)

    async def _create(self, force: bool) -> aiohttp.ClientResponse:
        metadata = dict(
            name=self.name,
            metadata={'expires': self._make_expired()},
        )
        body = '\r\n'.join([
            f'--{BOUNDARY}',
            'Content-Type: application/json; charset=UTF-8',
            '',
            json.dumps(metadata),
            f'--{BOUNDARY}',
            'Content-Type: plain/text',
            '',
            self.body,
            '',
            f'--{BOUNDARY}--',
            '',
        ])
        headers = await self._headers()
        headers.update({
            'Accept': 'application/json',
            'Content-Length': str(len(body)),
            'Content-Type': f'multipart/related; boundary={BOUNDARY}',
        })
        params = dict(uploadType='multipart')
        if not force:
            params['ifGenerationMatch'] = '0'
        return await self.session.post(
            url=f'{self.api_url}/upload/storage/v1/b/{self.bucket}/o',
            data=body.encode('utf8'),
            params=params,
            headers=headers,
        )

    async def _delete(self, generation: Optional[str] = None) -> aiohttp.ClientResponse:
        params = {}
        if generation is not None:
            params['ifGenerationMatch'] = generation
        return await self.session.delete(
            url=f'{self.api_url}/storage/v1/b/{self.bucket}/o/{quote_plus(self.name)}',
            params=params,
            headers=await self._headers(),
        )

    async def _get(self) -> aiohttp.ClientResponse:
        return await self.session.get(
            url=f'{self.api_url}/storage/v1/b/{self.bucket}/o/{quote_plus(self.name)}',
            headers=await self._headers(),
        )

    async def _patch(self) -> aiohttp.ClientResponse:
        metadata = dict(
            metadata={'expires': self._make_expired()},
        )
        return await self.session.patch(
            url=f'{self.api_url}/storage/v1/b/{self.bucket}/o/{quote_plus(self.name)}',
            headers=await self._headers(),
            json=metadata,
        )

    async def __aenter__(self) -> 'GCS':
        return self

    async def __aexit__(self, *args) -> None:
        # Check if the mutex is required but `acquire` or `acquired` was never used.
        # It allows to catch the error when the user assumes that entering
        # the context automatically locks the mutex.
        #
        # If you see this error, you must either:
        #   * call `acquire` or `acquired` at least once;
        #   * or pass `required=False` when creating the mutex.
        assert not self.required, 'lock is required but was not used'
        await self.session.close()
