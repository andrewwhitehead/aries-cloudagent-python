"""Transport utility methods."""

from aiohttp import BaseConnector, ClientError, ClientResponse, ClientSession

from ..task_processor import Repeat

from .error import TransportError


async def http_fetch(
    url: str,
    *,
    headers: dict = None,
    retry: bool = True,
    max_retries: int = 5,
    interval: float = 1.0,
    backoff: float = 0.25,
    request_timeout: float = 10.0,
    connector: BaseConnector = None,
    json: bool = False,
):
    """Fetch from an HTTP server with automatic retries and timeouts.

    Args:
        url: the address to fetch
        headers: an optional dict of headers to send
        retry: flag to retry the fetch
        max_retries: the maximum number of attempts to make
        interval: the interval between retries, in seconds
        backoff: the backoff interval, in seconds
        request_timeout: the HTTP request timeout, in seconds
        connector: an optional existing BaseConnector
        json: flag to parse the result as JSON

    """
    limit = max_retries if retry else 1
    async for task in Repeat.each(limit, interval, backoff):
        try:
            async with task.timeout(request_timeout):
                async with ClientSession(
                    connector=connector, connector_owner=(not connector)
                ) as client_session:
                    response: ClientResponse = await client_session.get(
                        url, headers=headers
                    )
                if response.status < 200 or response.status >= 300:
                    raise ClientError(f"Bad response from server: {response.status}")
                return await (response.json() if json else response.text())
        except (ClientError, Repeat.TimeoutError) as e:
            if task.final:
                raise TransportError("Exceeded maximum fetch attempts") from e
