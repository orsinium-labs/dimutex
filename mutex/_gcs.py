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
    bucket: str
    name: str
    api_url: str
    session: aiohttp.ClientSession
    token: Token
    emulator: bool
    ttl: timedelta
    now: Callable[..., datetime]

    def __init__(
        self,
        bucket: str,
        name: str,
        api_url: Optional[str] = None,
        session: Optional[aiohttp.ClientSession] = None,
        token: Optional[Token] = None,
        now: Optional[Callable[[], datetime]] = None,
        ttl: timedelta = timedelta(seconds=60),
    ) -> None:
        self.bucket = bucket
        self.name = name
        self.emulator = api_url is not None
        self.api_url = api_url or DEFAULT_URL
        self.ttl = ttl

        if now is None:
            now = datetime.utcnow
        self.now = now  # type: ignore
        if session is None:
            session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=not self.emulator),
                timeout=aiohttp.ClientTimeout(total=10),
            )
        self.session = session
        if token is None:
            token = Token(service_file=None, scopes=SCOPES, session=session)
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
        resp = await self._create(force=force)
        if resp.status == HTTPStatus.OK:
            return
        if resp.status == HTTPStatus.PRECONDITION_FAILED:
            if expired:
                await self._acquire_expired()
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

    async def _acquire_expired(self) -> None:
        """Acquire the lock if and only if the lock exists but expired.
        """
        resp = await self._get()
        resp.raise_for_status()
        content = await resp.json()
        expires = datetime.fromisoformat(content['metadata']['expires'])
        if self.now() < expires:
            raise AlreadyAcquiredError

        resp = await self._delete(generation=content['generation'])
        resp.raise_for_status()
        await self.acquire()
        return

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

    async def __aenter__(self) -> 'Token':
        return self

    async def __aexit__(self, *args) -> None:
        await self.session.close()
