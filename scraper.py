import asyncio
import logging
import datetime
import aiohttp
import pandas as pd
import json
import os

class CongressAPI:
    def __init__(self, concurrency_limit: int = 25, sleep_spacing: int = 1, max_retries: int = 3) -> None:
        """
        CongressAPI constructor.

        Args:
            concurrency_limit (int): Maximum number of concurrent requests.
            sleep_spacing (int): Time in seconds to sleep between requests.
            max_retries (int): Maximum number of retries for a failed request.
        """
        self.base_url = 'https://dadosabertos.camara.leg.br/api/v2/'
        self.concurrency_limit = asyncio.Semaphore(concurrency_limit)
        self.sleep_spacing = sleep_spacing
        self.max_retries = max_retries
        self.sleep_until = datetime.datetime.now()

        # Create logs directory if it doesn't exist
        log_dir = 'logs'
        os.makedirs(log_dir, exist_ok=True)

        # Configure logger
        log_file = os.path.join(log_dir, f'congress_api_{datetime.datetime.now()}.log')
        logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger('CongressAPI')

    async def check_rate_limit(self, retry_after):
        """
        Check and handle rate limiting.

        Args:
            retry_after (int): Number of seconds to wait before making the next request.
        """
        self.sleep_until = max(datetime.datetime.now() + datetime.timedelta(seconds=retry_after), self.sleep_until)
        sleep_until_str = self.sleep_until.strftime("%H:%M:%S")

        if self.sleep_until > datetime.datetime.now():
            self.logger.warning(f"Rate limit exceeded. Sleeping until {sleep_until_str}")
            await asyncio.sleep((self.sleep_until - datetime.datetime.now()).total_seconds())

    async def make_request(self, session: aiohttp.ClientSession, url: str, headers: dict = None, params: dict = None) -> dict:
        """
        Make an asynchronous HTTP request.

        Args:
            session: Aiohttp ClientSession instance.
            url (str): The URL to make the request to.
            headers (dict): Request headers.
            params (dict): Query parameters.

        Returns:
            dict: JSON response.
        """
        for retry in range(self.max_retries):
            try:
                async with self.concurrency_limit:
                    await self.check_rate_limit(0)

                    async with session.get(url, headers=headers, params=params) as response:
                        if response.status == 429:
                            await self.check_rate_limit(int(response.headers.get('Retry-After')))
                            continue

                        response.raise_for_status()
                        await asyncio.sleep(self.sleep_spacing)
                        return await response.json()

            except aiohttp.ClientResponseError as e:
                self.logger.error(f"Failed to fetch data from {url}. HTTP status: {e.status}")
                if retry < self.max_retries - 1:
                    self.logger.info(f"Retrying... ({retry + 1}/{self.max_retries})")
                    continue
                else:
                    self.logger.error("Max retry limit reached. Giving up.")
                    return None
            except aiohttp.ClientError as e:
                self.logger.error(f"An error occurred while fetching data from {url}: {e}")
                return None

    async def fetch_data_for_endpoint(self, session: aiohttp.ClientSession, endpoint: str, params: dict) -> list | None:
        """
        Generic method to fetch data for a given endpoint asynchronously.

        Args:
            session: Aiohttp ClientSession instance.
            endpoint: The API endpoint to fetch data from.
            params: Dictionary of parameters to include in the request.

        Returns:
            list: List of fetched data.
        """
        url = f'{self.base_url}{endpoint}'
        headers = {'accept': 'application/json'}
        data_list = []

        self.logger.info(f"Fetching data for {endpoint}...")

        # Make the first request
        initial_data = await self.make_request(session, url, headers=headers, params=params)
        
        if not initial_data or not initial_data.get('dados'):
            self.logger.warning(f"No data found for {endpoint}")
            return None

        data_list.extend(initial_data['dados'])
        
        # Get next pages
        links = initial_data.get('links', {})
        last_page_link = links.get('last')
        last_page_id = int(last_page_link.split('=')[-1])
        all_pages_ids = list(range(2, last_page_id + 1))

        # Fetch subsequent pages asynchronously
        tasks = []
        for next_page_link in all_pages_ids:
            params['pagina'] = next_page_link
            tasks.append(self.make_request(session, f'{self.base_url}{endpoint}', headers=headers, params=params))
            
        next_pages_data = await asyncio.gather(*tasks)
        for page_data in next_pages_data:
            if page_data:
                data_list.extend(page_data['dados'])
        
        return data_list
    
    async def fetch_congresspeople_ids(self, session: aiohttp.ClientSession) -> list:
        """
        Fetch congresspeople IDs asynchronously.

        Args:
            session: Aiohttp ClientSession instance.

        Returns:
            list: List of congresspeople IDs.
        """
        params = {'ordem': 'ASC', 'ordenarPor': 'nome', 'itens': 1000, 'idLegislatura': f'{list(range(51, 58))}'}
        return await self.fetch_data_for_endpoint(session, 'deputados', params)

    async def fetch_congressperson_details(self, session: aiohttp.ClientSession, congressperson_id: int) -> dict:
        """
        Fetch details of a congressperson asynchronously.

        Args:
            session: Aiohttp ClientSession instance.
            congressperson_id (int): ID of the congressperson.

        Returns:
            dict: JSON response containing details of the congressperson.
        """
        headers = {'accept': 'application/json'}

        try:
            data = await self.make_request(session, f'{self.base_url}deputados/{congressperson_id}', headers=headers)
            self.logger.info(f"Fetched data for congressperson {congressperson_id}...")
        except Exception as e:
            self.logger.error(f"Could not fetch data for congressperson {congressperson_id}. Error: {e}")
            return None

        return data['dados']

    async def get_all_congresspeople_details(self) -> list:
        """
        Fetch details of all congresspeople asynchronously.

        Returns:
            list: List of JSON responses containing details of all congresspeople.
        """
        async with aiohttp.ClientSession() as session:
            congresspeople_ids = await self.fetch_congresspeople_ids(session)
            tasks = [self.fetch_congressperson_details(session, congressperson_id) for congressperson_id in congresspeople_ids]
            congresspeople_details = await asyncio.gather(*tasks)

        return congresspeople_details
    
    def get_congresspeople(self) -> list:
        return asyncio.run(self.get_all_congresspeople_details())



if __name__ == '__main__':
    api = CongressAPI()
    congresspeople = api.get_congresspeople()

    # Create data and raw inside data directory if they doesn't exist
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)
    raw_dir = os.path.join(data_dir, 'raw')
    os.makedirs(raw_dir, exist_ok=True)

    # Save data as JSON
    congresspeople_file = os.path.join(raw_dir, f'congresspeople_{datetime.datetime.now()}.json')
    with open(congresspeople_file, 'w') as f:
        json.dump(congresspeople, f)
