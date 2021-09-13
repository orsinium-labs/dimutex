from typing import Callable, Dict, Optional
import aiohttp
from gcloud.aio.auth import Token
from http import HTTPStatus
from ._exceptions import AlreadyAcquiredError, AlreadyReleasedError
from urllib.parse import quote
import json
from datetime import datetime, timedelta


DEFAULT_URL = 'https://www.googleapis.com'
SCOPES = [
    'https://www.googleapis.com/auth/devstorage.read_write',
]
BOUNDARY = 'cf58b63b6ce6f37881e9740f24be22d7'


class GCS:
    """

    Args:
        bucket: GCS bucket name.
        name: lock name, used as filename of lock in GCS.
        api_url: URL of GCS API, helpful for testing with emulator.
        now: callback used to determine the current time, helpful for tesing.
        ttl: how long to wait before the lock considered to be stale.
        required: if True, `acquire` must be called at least once.
    """
    bucket: str
    name: str
    api_url: str
    session: aiohttp.ClientSession
    token: Token
    emulator: bool
    ttl: timedelta
    now: Callable[..., datetime]
    required: bool

    def __init__(
        self,
        bucket: str,
        name: str,
        api_url: Optional[str] = None,
        session: Optional[aiohttp.ClientSession] = None,
        token: Optional[Token] = None,
        now: Callable[[], datetime] = datetime.utcnow,
        ttl: timedelta = timedelta(seconds=60),
        required: bool = True,
    ) -> None:
        self.bucket = bucket
        self.name = name
        self.emulator = api_url is not None
        self.api_url = api_url or DEFAULT_URL
        self.ttl = ttl
        self.now = now  # type: ignore
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
        resp = await self._delete()
        if resp.status == HTTPStatus.NOT_FOUND:
            raise AlreadyReleasedError
        resp.raise_for_status()

    async def _release_expired(self) -> None:
        """Acquire the lock if and only if the lock exists but expired.
        Raises:
            ClientResponseError
            AlreadyAcquiredError
        """
        resp = await self._get()
        if resp.status == HTTPStatus.NOT_FOUND:
            return
        resp.raise_for_status()
        content = await resp.json()
        expires = datetime.fromisoformat(content['metadata']['expires'])
        if self.now() < expires:
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

    async def _create(self, force: bool) -> aiohttp.ClientResponse:
        metadata = dict(
            name=self.name,
            metadata={
                'expires': (self.now() + self.ttl).isoformat(),
            },
        )
        body = '\r\n'.join([
            f'--{BOUNDARY}',
            'Content-Type: application/json; charset=UTF-8',
            '',
            json.dumps(metadata),
            f'--{BOUNDARY}',
            'Content-Type: plain/text',
            '',
            'lock',
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
            url=f'{self.api_url}/storage/v1/b/{self.bucket}/o/{quote(self.name)}',
            params=params,
            headers=await self._headers(),
        )

    async def _get(self) -> aiohttp.ClientResponse:
        return await self.session.get(
            url=f'{self.api_url}/storage/v1/b/{self.bucket}/o/{quote(self.name)}',
            headers=await self._headers(),
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
