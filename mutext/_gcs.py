from typing import Dict, Optional
import aiohttp
from gcloud.aio.auth import Token
from http import HTTPStatus
from ._exceptions import AlreadyAcquiredError
from urllib.parse import quote


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

    def __init__(
        self,
        bucket: str,
        name: str,
        api_url: Optional[str] = None,
        session: Optional[aiohttp.ClientSession] = None,
        token: Optional[Token] = None,
    ) -> None:
        self.bucket = bucket
        self.name = name
        self.emulator = api_url is not None
        self.api_url = api_url or DEFAULT_URL

        if session is None:
            session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=not self.emulator),
                timeout=10,
            )
        if token is None:
            token = Token(service_file=None, scopes=SCOPES, session=session)
        self.session = session
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
            raise AlreadyAcquiredError()
        resp.raise_for_status()

    async def release(self) -> None:
        """Release (unlock) the mutex

        Raises:
            ClientResponseError
        """
        resp = await self._delete()
        resp.raise_for_status()

    async def _create(self) -> aiohttp.ClientResponse:
        headers = await self._headers()
        headers.update({
            'Content-Length': '0',
            'Content-Type': '',
            'x-goog-if-generation-match': '0',
        })
        return await self.session.post(
            url=f'{self.api_url}/upload/storage/v1/b/{self.bucket}/o',
            data=b'',
            params=dict(
                name=self.name,
                uploadType='media',
            ),
            headers=headers,
        )

    async def _delete(self, generation: Optional[str] = None) -> aiohttp.ClientResponse:
        headers = await self._headers()
        if generation is not None:
            headers['x-goog-if-generation-match'] = generation
        return await self.session.delete(
            url=f'{self.api_url}/storage/v1/b/{self.bucket}/o/{quote(self.name)}',
            headers=headers,
        )

    async def __aenter__(self) -> 'Token':
        return self

    async def __aexit__(self, *args) -> None:
        await self.session.close()
