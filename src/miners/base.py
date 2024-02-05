"""
Base class for miners.
"""

import os
import uuid
import asyncio
import logging
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from aiohttp import ClientSession


class BaseMiner(ABC):
    """
    Base class for miners.
    """

    def __init__(
        self,
        name: str,
        log_level: int = logging.INFO,
        log_file: str = "logs/base.log",
        output_path: str = "data/base",
        **kwargs,
    ):
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
        os.makedirs(f"{output_path}/", exist_ok=True)
        self.max_retries = kwargs.get("max_retries", 3)
        self.concurrency = kwargs.get("concurrency", asyncio.Semaphore(10000))
        self.wait_until = datetime.now()
        self.logger = BaseLogger(name=self.name, log_level=log_level, log_file=log_file)

    async def check_rate_limit(self, retry_after: int) -> None:
        """
        Check and handle rate limiting.

        Args:
            retry_after (int): Number of seconds to wait before making the next request.
        """
        self.wait_until = max(
            datetime.now() + timedelta(seconds=retry_after), self.wait_until
        )
        wait_until_str = self.wait_until.strftime("%H:%M:%S")

        if self.wait_until > datetime.now():
            self.logger.warning(
                "Rate limit exceeded. Sleeping until %s", wait_until_str
            )
            await asyncio.sleep((self.wait_until - datetime.now()).total_seconds())
            self.logger.warning(
                "Resuming mining at %s", datetime.now().strftime("%H:%M:%S")
            )

    async def make_request(
        self, url: str, session: ClientSession, headers: dict = {}, params: dict = {}
    ) -> dict:
        """
        Make a request to the API.

        Args:
            url (str): URL of the API.
            session (ClientSession): Session to use.
            headers (dict, optional): Headers to include in the request. Defaults to {}.
            params (dict, optional): Parameters to include in the request. Defaults to {}.

        Returns:
            dict: Response from the API.
        """
        request_id = uuid.uuid4()
        self.logger.info("Making request to %s - Request ID: %s", url, request_id)
        response = None
        for retry in range(self.max_retries):
            await self.check_rate_limit(0)
            self.logger.debug(
                "Making request to %s - Request ID: %s. Params: %s - Headers: %s - Retry: %s/%s",
                url,
                request_id,
                params,
                headers,
                retry,
                self.max_retries,
            )
            try:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status in [200, 201]:
                        self.logger.debug(
                            "Request to %s successful - Request ID: %s",
                            url,
                            request_id,
                        )
                        return await response.json()
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 0))
                        await self.check_rate_limit(retry_after)
            except Exception as e:
                self.logger.warning(
                    "Error in request to %s - Request ID: %s. Error: %s",
                    url,
                    request_id,
                    e,
                )

        self.logger.error(
            "Max retries reached for %s - Request ID: %s", url, request_id
        )
        return {}

    async def fetch_all_pages(
        self, url: str, headers: dict, params: dict, session: ClientSession
    ) -> list:
        """
        Fetch all pages of a paginated API.

        Args:
            url (str): URL of the API.
            headers (dict, optional): Headers to include in the request.
                Defaults to None.
            params (dict, optional): Parameters to include in the request.
                Defaults to None.
            session (ClientSession, optional): Session to use.
                Defaults to None.

        Returns:
            list: List of responses.
        """
        response = await self.make_request(
            url=url, session=session, headers=headers, params=params
        )
        links = response.get("links", {})
        if not links or len(links) < 2:
            return [response]

        last_page_id = int(links[-1]["href"].split("&pagina=")[1].split("&")[0])

        # Fetch subsequent pages asynchronously
        tasks = []
        for next_page_link in list(range(2, last_page_id + 1)):
            page_params = params.copy()
            page_params["pagina"] = next_page_link
            tasks.append(
                self.make_request(
                    url, session=session, headers=headers, params=page_params
                )
            )
        next_pages_data = await asyncio.gather(*tasks)
        log_message = "Fetched %d pages of data for %s and params %s"
        self.logger.info(log_message, len(next_pages_data) + 1, url, params)
        data = response.get("dados", [])
        data.extend([page.get("dados", []) for page in next_pages_data])

        return data

    async def fetch_endpoint_list(
        self,
        url: str,
        path_parameters: list[str],
        expected_data_key: str,
        headers: dict,
    ) -> dict[str, dict]:
        """
        Fetch details of a list of urls asynchronously.
        Same endpoint, different IDs.

        Args:
            url (str): Target URL with endpoint.
            path_parameters (list): List of path parameters.
            expected_data_key (str): Expected data key in the response.
        Returns:
            dict: Dictionary containing the data. Keys are the path parameters. Values are the data.
        """
        data = {}
        for path_parameter in path_parameters:
            query = f"{url}/{path_parameter}"
            async with self.concurrency:
                async with ClientSession() as session:
                    response = await self.make_request(query, session, headers)
                    if expected_data_key in response:
                        data[path_parameter] = response[expected_data_key]
                        self.logger.info("Fetched data %s from %s", path_parameter, url)
                    else:
                        self.logger.error(
                            "Failed to fetch data %s from %s", path_parameter, url
                        )
        return data

    @abstractmethod
    def mine(self):
        """
        Abstract method to define the mining process.
        """


class BaseLogger(logging.Logger):
    """
    Base class for loggers. Inherits from the logging.Logger class.
    Deals with logging configurations.
    """

    def __init__(self, name, log_level: int, log_file: str, **kwargs):
        """
        Initialize the BaseLogger.

        Args:
            log_level (int): Logging level.
            log_file (str): Path to the log file.
            **kwargs: Additional arguments.
        """
        super().__init__(kwargs.get("logger", "BaseLogger"))
        self.handle_logging(name, log_level, log_file, kwargs.get("terminal", False))

    def handle_logging(self, name, log_level, log_file, terminal) -> None:
        """
        Set up logging configurations.

        Args:
            log_level (int): Logging level.
            log_file (str): Path to the log file.
            terminal (bool): Whether to log to the terminal.
        """
        self.log_level = log_level
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        parts = log_file.split("/")
        parts[-1] = f"%s-%s" % (datetime.now().strftime("%Y-%m-%d@%H:%M:%S"), parts[-1])
        self.log_file = "/".join(parts)
        self.setLevel(self.log_level)
        self.addHandler(logging.FileHandler(self.log_file))
        if terminal:
            self.addHandler(logging.StreamHandler())
        self.info(
            "Initializing logger %s - Log level: %s - Log file: %s",
            name,
            logging.getLevelName(self.log_level),
            self.log_file,
        )


# Example usage:
class MyMiner(BaseMiner):
    """
    Example miner class.
    """

    def __init__(self, **kwargs):
        super().__init__(
            name="MyMiner", log_level=logging.DEBUG, log_file="miner.log", **kwargs
        )

    def mine(self):
        """
        Example mining process.
        """
        # Implement your mining logic here


# Instantiate and use your miner
if __name__ == "__main__":
    miner = MyMiner()
