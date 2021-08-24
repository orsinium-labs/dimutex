from typing import Callable, Dict, Optional
import aiohttp
from gcloud.aio.auth import Token
from http import HTTPStatus
from ._exceptions import AlreadyAcquiredError
from urllib.parse import quote
from urllib3.filepost import encode_multipart_formdata
import json
from datetime import datetime, timedelta


DEFAULT_URL = 'https://www.googleapis.com'
SCOPES = [
    'https://www.googleapis.com/auth/devstorage.read_write',
]


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
                timeout=10,
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

    async def acquire(self) -> None:
        """Acquire (lock) the mutex.

        Raises:
            AlreadyAcquiredError
            ClientResponseError
        """
        resp = await self._create()
        if resp.status == HTTPStatus.OK:
            return
        if resp.status == HTTPStatus.PRECONDITION_FAILED:
            content = await resp.json()
            expires = datetime.fromisoformat(content['metadata']['expires'])
            if self.now() >= expires:
                resp = await self._delete(generation=content['generation'])
                resp.raise_for_status()
                await self.acquire()
                return
            raise AlreadyAcquiredError()
        resp.raise_for_status()

    async def reacquire(self) -> None:
        """Release (unlock) the mutex even if it is already locked.

        Raises:
            ClientResponseError
        """
        await self.release()
        await self.acquire()

    async def release(self) -> None:
        """Release (unlock) the mutex

        Raises:
            ClientResponseError
        """
        resp = await self._delete()
        resp.raise_for_status()

    async def _create(self) -> aiohttp.ClientResponse:
        metadata = {
            'name': self.name,
            'expires': (self.now() + self.ttl).isoformat(),
        }
        body, content_type = encode_multipart_formdata([
            (
                {'Content-Type': 'application/json; charset=UTF-8'},
                json.dumps(metadata).encode('utf-8'),
            ),
            (
                {'Content-Type': 'text/plain'},
                b'',
            ),
        ])
        headers = await self._headers()
        headers.update({
            'Accept': 'application/json',
            'Content-Length': '0',
            'Content-Type': content_type,
        })
        return await self.session.post(
            url=f'{self.api_url}/upload/storage/v1/b/{self.bucket}/o',
            data=body,
            params=dict(
                uploadType='multipart',
                ifGenerationMatch='0',
            ),
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

    async def __aenter__(self) -> 'Token':
        return self

    async def __aexit__(self, *args) -> None:
        await self.session.close()
