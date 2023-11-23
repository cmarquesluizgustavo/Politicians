import asyncio
import logging
import datetime
import aiohttp
import uuid
import json
import os

class CongressAPI:
    def __init__(self, concurrency_limit: int = 25, sleep_spacing: int = 1, 
                 max_retries: int = 3, log_level: int = logging.INFO) -> None:
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
        logging.basicConfig(filename=log_file, level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger('CongressAPI')
        self.logger.info(f'CongressAPI initialized. - Log level: {logging.getLevelName(log_level)}')

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

    async def make_request(self, url: str, headers: dict = None, params: dict = None) -> dict:
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
        request_id = uuid.uuid4()
        for retry in range(self.max_retries):
            async with aiohttp.ClientSession() as session:
                try:
                    async with self.concurrency_limit:
                        await self.check_rate_limit(0)
                        self.logger.debug(f"URL: {url}, params: {params}, headers: {headers} - Request ID: {request_id} | Retry: {retry + 1}/{self.max_retries}")
                        async with session.get(url, headers=headers, params=params) as response:
                            if response.status == 429:
                                await self.check_rate_limit(int(response.headers.get('Retry-After')))
                                continue

                            response.raise_for_status()
                            await asyncio.sleep(self.sleep_spacing)
                            logging.debug(f"Request ID: {request_id} - Response status: {response.status}")
                            return await response.json()

                except aiohttp.ClientResponseError as e:
                    self.logger.error(f"Failed to fetch data from {url}. HTTP status: {e.status}")
                    if retry < self.max_retries - 1:
                        self.logger.warning(f"Retrying request {retry + 1}/{self.max_retries} - Request ID: {request_id}...")
                        continue
                    else:
                        self.logger.error(f"Max retry limit reached. Giving up. - Request ID: {request_id}")
                        raise e
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
        initial_data = await self.make_request(url, headers=headers, params=params, session=session)
        
        if not initial_data or not initial_data.get('dados'):
            self.logger.warning(f"No data found for {endpoint}")
            return None

        self.logger.info(f"Fetched first page of data for {endpoint}...")
        data_list.extend(initial_data['dados'])
        
        # Get next pages
        links = initial_data.get('links', {})
        if not links or len(links) < 2:
            return data_list
        
        last_page_link = links[-1]
        last_page_id = int(last_page_link['href'].split('&pagina=')[1].split('&')[0])
        all_pages_ids = list(range(2, last_page_id + 1))

        # Fetch subsequent pages asynchronously
        tasks = []
        for next_page_link in all_pages_ids:
            page_params = params.copy()
            page_params['pagina'] = next_page_link
            tasks.append(self.make_request(f'{self.base_url}{endpoint}', headers=headers, params=page_params, session=session))
        next_pages_data = await asyncio.gather(*tasks)
        self.logger.info(f"Fetched {len(next_pages_data) + 1} pages of data for {endpoint}.")
        for page_data in next_pages_data:
            if page_data:
                data_list.extend(page_data['dados'])
        
        return data_list
    
    async def fetch_congresspeople_basic_info(self) -> list:
        """
        Fetch congresspeople basic infos asynchronously.

        Args:
            session: Aiohttp ClientSession instance.

        Returns:
            list: List of congresspeople IDs.
        """
        legislatures_ids = list(range(51, 58))
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_data_for_endpoint(session, 'deputados', 
                                                {'ordem': 'ASC', 'ordenarPor': 'nome', 'itens': 1000, 'idLegislatura': legislatures_id}) 
                                                for legislatures_id in legislatures_ids]
            congresspeople_basic_infos = await asyncio.gather(*tasks)

        congresspeople_basic_infos = [congressperson for congresspeople in congresspeople_basic_infos for congressperson in congresspeople]
        return congresspeople_basic_infos

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
            data = await self.make_request(session, f'{self.base_url}deputados/{congressperson_id}', headers=headers, session=session)
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
        congresspeople_basic_info = await self.fetch_congresspeople_basic_info()
        congresspeople_ids = [congressperson['id'] for congressperson in congresspeople_basic_info]
        congresspeople_ids = list(set(congresspeople_ids))
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_congressperson_details(session, congressperson_id) for congressperson_id in congresspeople_ids]
            logging.info(f"Fetching details for {len(congresspeople_ids)} congresspeople...")
            congresspeople_details = await asyncio.gather(*tasks)

        # Add total status to each congressperson
        logging.info(f"Adding total status to each congressperson. Total: {len(congresspeople_details)}")
        counter = 0
        for congressperson_detail in congresspeople_details:
            congressperson_detail['total_status'] = {
                'siglaUf': {congressperson_detail['ultimoStatus']['siglaUf']},
                'siglaPartido': {congressperson_detail['ultimoStatus']['siglaPartido']},
                'idLegislatura': {congressperson_detail['ultimoStatus']['idLegislatura']}
            }
            for congressperson_basic_info in congresspeople_basic_info:
                if congressperson_basic_info['id'] == congressperson_detail['id']:
                    congressperson_detail['total_status']['siglaUf'].add(congressperson_basic_info['siglaUf'])
                    congressperson_detail['total_status']['siglaPartido'].add(congressperson_basic_info['siglaPartido'])
                    congressperson_detail['total_status']['idLegislatura'].add(congressperson_basic_info['idLegislatura'])
                    congresspeople_basic_info.remove(congressperson_basic_info)
            # Remove sets
            congressperson_detail['total_status']['siglaUf'] = list(congressperson_detail['total_status']['siglaUf'])
            congressperson_detail['total_status']['siglaPartido'] = list(congressperson_detail['total_status']['siglaPartido'])
            congressperson_detail['total_status']['idLegislatura'] = list(congressperson_detail['total_status']['idLegislatura'])

            if counter % 100 == 0:
                self.logger.info(f"Added total status to {counter} congresspeople...")
            counter += 1
        return congresspeople_details
    
    def get_congresspeople(self) -> list:
        return asyncio.run(self.get_all_congresspeople_details())


if __name__ == '__main__':
    api = CongressAPI()
    congresspeople = api.get_congresspeople()

    os.makedirs('data', exist_ok=True)
    os.makedirs('data/raw', exist_ok=True)

    with open(os.path.join(f'data/raw/congresspeople_{datetime.datetime.now()}.json'), 'w') as f:
        json.dump(congresspeople, f, indent=4)
