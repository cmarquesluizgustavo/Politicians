import asyncio
import aiohttp
import logging
import uuid
from datetime import datetime, timedelta
from abc import ABC, abstractmethod


class BaseMiner(ABC):
    def __init__(self, name: str, log_level: int = logging.INFO, log_file: str = None, **kwargs):
        """
        Initialize the BaseMiner.

        Args:
            name (str): Name of the miner.
            log_level (int): Logging level.
            log_file (str): Path to the log file.
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
                self.logger.debug(
                    f"Making request to {url} - Request ID: {request_id}. Params: {params} - Headers: {headers} - Retry: {retry}/{self.max_retries}")
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status in [200, 201]:
                        self.logger.debug(f"Request to {url} successful - Request ID: {request_id}")
                        return await response.json()
                    elif response.status == 429:
                        retry_after = int(response.headers.get('Retry-After', 0))
                        await self.check_rate_limit(retry_after)

        self.logger.error(f"Error in response from {url}. Status code: {response.status} - Request ID: {request_id}")

    async def fetch_all_pages(self, url: str, headers: dict = None, params: dict = None,
                              session: aiohttp.ClientSession = None) -> list:
        """
        Fetch all pages of a paginated API.

        Args:
            url (str): URL of the API.
            headers (dict, optional): Headers to include in the request. Defaults to None.
            params (dict, optional): Parameters to include in the request. Defaults to None.
            session (aiohttp.ClientSession, optional): Session to use. Defaults to None.

        Returns:
            list: List of responses.
        """
        response = await self.make_request(url=url, headers=headers, params=params, session=session)
        links = response.get('links', {})
        if not links or len(links) < 2:
            return [response]

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
        """
        Abstract method to define the mining process.
        """
        pass


class BaseLogger(logging.Logger):
    def __init__(self, log_level: int = logging.INFO, log_file: str = None, **kwargs):
        """
        Initialize the BaseLogger.

        Args:
            log_level (int): Logging level.
            log_file (str): Path to the log file.
            **kwargs: Additional arguments.
        """
        super().__init__(kwargs.get('logger', 'BaseLogger'))
        self.handle_logging(log_level, log_file, kwargs.get('terminal', False))

    def handle_logging(self, log_level, log_file, terminal) -> None:
        """
        Set up logging configurations.

        Args:
            log_level (int): Logging level.
            log_file (str): Path to the log file.
            terminal (bool): Whether to log to the terminal.
        """
        self.log_level = log_level
        self.log_file = log_file
        self.setLevel(self.log_level)
        self.addHandler(logging.FileHandler(self.log_file))
        if terminal:
            self.addHandler(logging.StreamHandler())
        self.info(f"Initializing logger - Log level: {self.log_level} - Log file: {self.log_file}")


# Example usage:
class MyMiner(BaseMiner):
    async def mine(self):
        # Implement your mining logic here
        pass


# Instantiate and use your miner
miner = MyMiner(name="MyMiner", log_level=logging.DEBUG, log_file="miner.log")
