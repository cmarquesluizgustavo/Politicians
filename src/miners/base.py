from abc import ABC, abstractmethod
from src.utils import BaseLogger
import asyncio
import uuid
import aiohttp
from datetime import datetime, timedelta
import logging

class BaseMiner(ABC):
    def __init__(self, name: str, log_level: int = 10, log_file: str = None, **kwargs):
        """
        Initialize the BaseMiner.

        Args:
            name (str): Name of the miner.
            base_url (str): Base URL of the API.
            endpoint (str): Endpoint to mine.
            **kwargs: Additional arguments.
        """
        self.name = name
        self.kwargs = kwargs
        self.max_retries = kwargs.get('max_retries', 3)
        self.concurrency = asyncio.Semaphore(kwargs.get('concurrency', 5))
        self.wait_until = datetime.now()
        self.logger = BaseLogger(name=self.name, log_level=log_level, log_file=log_file)

    async def check_rate_limit(self, retry_after: int) -> None:
        """
        Check and handle rate limiting.

        Args:
            retry_after (int): Number of seconds to wait before making the next request.
        """
        self.wait_until = max(datetime.now() + timedelta(seconds=retry_after), self.wait_until)
        wait_until_str = self.wait_until.strftime("%H:%M:%S")

        if self.wait_until > datetime.now():
            self.logger.warning(f"Rate limit exceeded. Sleeping until {wait_until_str}")
            await asyncio.sleep((self.wait_until - datetime.now()).total_seconds())
            self.logger.warning(f"Resuming mining at {datetime.now().strftime('%H:%M:%S')}")
    
    async def make_request(self, url: str, headers: dict = None, params: dict = None, session: aiohttp.ClientSession = None) -> dict:
        """
        Make a request to the API.

        Args:
            url (str): URL to make the request to.
            headers (dict, optional): Headers to include in the request. Defaults to None.
            params (dict, optional): Parameters to include in the request. Defaults to None.

        Returns:
            dict: Response from the API.
        """
        request_id = uuid.uuid4()
        self.logger.info(f"Making request to {url} - Request ID: {request_id}")
        async with self.concurrency:
            for retry in range(self.max_retries):
                await self.check_rate_limit(0)
                self.logger.debug(f"Making request to {url} - Request ID: {request_id}. Params: {params} - Headers: {headers} - Retry: {retry}/{self.max_retries}")
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status in [200, 201]:
                        self.logger.debug(f"Request to {url} successful - Request ID: {request_id}")
                        return await response.json()
                    elif response.status == 429:
                        retry_after = int(response.headers.get('Retry-After', 0))
                        await self.check_rate_limit(retry_after)
                
        self.logger.error(f"Error in response from {url}. Status code: {response.get('status_code')} - Request ID: {request_id}")

    async def fetch_all_pages(self, url: str, headers: dict = None, params: dict = None, session: aiohttp.ClientSession = None) -> list:
        """
        Fetch all pages of a paginated API.

        Args:
            links (list): List of links to fetch.
            session (aiohttp.ClientSession, optional): Session to use. Defaults to None.

        Returns:
            list: List of responses.
        """
        response = await self.make_request(url=url, headers=headers, params=params, session=session)
        links = response.get('links', {})
        if not links or len(links) < 2:
            return [response]
        
        # Get next pages
        links = response.get('links', {})
        if not links or len(links) < 2:
            return response.get('dados', [])
        
        last_page_id = int(links[-1]['href'].split('&pagina=')[1].split('&')[0])

        # Fetch subsequent pages asynchronously
        tasks = []
        for next_page_link in list(range(2, last_page_id + 1)):
            page_params = params.copy()
            page_params['pagina'] = next_page_link
            tasks.append(self.make_request(url, headers=headers, params=page_params, session=session))
        next_pages_data = await asyncio.gather(*tasks)
        self.logger.info(f"Fetched {len(next_pages_data) + 1} pages of data for {url}.")

        data = response.get('dados', [])
        data.extend([page.get('dados', []) for page in next_pages_data])

        return data

    @abstractmethod
    def mine(self):
        pass

    

