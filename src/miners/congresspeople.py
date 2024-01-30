import os
import asyncio
import logging
import aiohttp
import pandas as pd
from base import BaseMiner

class CongressPeople(BaseMiner):
    def __init__(self) -> None:
        """
        CongressAPI constructor.

        Args:
            concurrency_limit (int): Maximum number of concurrent requests.
            sleep_spacing (int): Time in seconds to sleep between requests.
            max_retries (int): Maximum number of retries for a failed request.
        """
        super().__init__(name='Authors', log_file='logs/congresspeople.log', terminal=True)
        self.output_path = "data/congresspeople/"
        self.base_url = 'https://dadosabertos.camara.leg.br/api/v2/'
        os.makedirs(self.output_path, exist_ok=True)

        # Configure logger
        
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
            tasks = [self.fetch_all_pages(f'{self.base_url}deputados', params={'idLegislatura': legislature_id}, session=session) for legislature_id in legislatures_ids]
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
            data = await self.make_request(f'{self.base_url}deputados/{congressperson_id}', headers=headers, session=session)
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
        counter = 0
        for congressperson in congresspeople_basic_info:
            if type(congressperson) == list:
                for c in congressperson:
                    if 'id' in c:
                        congresspeople_basic_info.append(c)
                congresspeople_basic_info.remove(congressperson)

        congresspeople_ids = [congressperson['id'] for congressperson in congresspeople_basic_info]
        congresspeople_ids = list(set(congresspeople_ids))
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_congressperson_details(session, congressperson_id) for congressperson_id in congresspeople_ids]
            logging.info(f"Fetching details for {len(congresspeople_ids)} congresspeople...")
            congresspeople_details = await asyncio.gather(*tasks)
        
        # Add total status to each congressperson
        logging.info(f"Adding total status to each congressperson. Total: {len(congresspeople_details)}")
        counter = 0
        result_congresspeople_details = pd.DataFrame()
        for congressperson_detail in congresspeople_details:
            try:
                this_congressperson_term = pd.DataFrame.from_dict(congressperson_detail, orient='index').T
                this_congressperson_term['siglaUf']       = this_congressperson_term['ultimoStatus'].apply(lambda x: x['siglaUf'])
                this_congressperson_term['siglaPartido']  = this_congressperson_term['ultimoStatus'].apply(lambda x: x['siglaPartido'])
                this_congressperson_term['nomeEleitoral'] = this_congressperson_term['ultimoStatus'].apply(lambda x: x['nomeEleitoral'])
                this_congressperson_term['idLegislatura'] = this_congressperson_term['ultimoStatus'].apply(lambda x: x['idLegislatura'])
                this_congressperson_term                  = this_congressperson_term[['id', 'nomeCivil', 'ultimoStatus', 'escolaridade', 'cpf',
                                                                                      'redeSocial', 'dataNascimento', 'ufNascimento', 'sexo',
                                                                                      'siglaUf', 'siglaPartido', 'nomeEleitoral', 'idLegislatura']]
                result_congresspeople_details = pd.concat([result_congresspeople_details, this_congressperson_term])
            except:
                self.logger.error(f"Could not add inital status to congressperson {counter}")
                raise
            try:
                # Add other legislatures
                for congressperson_basic_info in congresspeople_basic_info:
                    if congressperson_basic_info['id'] == congressperson_detail['id']:
                        idlegislatura = int(congressperson_basic_info['idLegislatura'])
                        if idlegislatura in result_congresspeople_details['idLegislatura']:
                            continue
                        this_congressperson_term['siglaUf']       = congressperson_basic_info['siglaUf']
                        this_congressperson_term['siglaPartido']  = congressperson_basic_info['siglaPartido']
                        this_congressperson_term['idLegislatura'] = idlegislatura
                        congresspeople_basic_info.remove(congressperson_basic_info)
                        result_congresspeople_details = pd.concat([result_congresspeople_details, this_congressperson_term])
            except:
                self.logger.error(f"Could not add other legislatures to congressperson {counter}")
                raise
            if counter % 100 == 0:
                self.logger.info(f"Added total status to {counter + 1} congresspeople...")
            counter += 1
        return result_congresspeople_details
    
    def create_dataframe(self, congresspeople: list) -> pd.DataFrame:
        """
        Create a dataframe from a list of congresspeople.

        Args:
            congresspeople (list): List of congresspeople.

        Returns:
            pd.DataFrame: Dataframe containing the congresspeople.
        """
        df = pd.DataFrame(congresspeople)
        msg = f"Created dataframe with {len(df)} congresspeople. {len(congresspeople)} were expected."
        self.logger.info(msg)
        df = df[['id', 'nomeCivil', 'nomeEleitoral', 'ufNascimento', 'escolaridade', 'dataNascimento',
                 'sexo', 'cpf', 'redeSocial', 'siglaUf', 'siglaPartido', 'idLegislatura']]
        df.to_csv('data/congresspeople/congresspeople.csv', index=False, encoding='utf-8')
        return df
    
    def mine(self) -> pd.DataFrame:
        """
        Mine congresspeople data.
        """
        self.logger.info("Mining congresspeople data...")
        congresspeople = asyncio.run(self.get_all_congresspeople_details())
        congresspeople = self.create_dataframe(congresspeople)
        self.logger.info(f"Finished mining congresspeople data. {len(congresspeople)} congresspeople were mined.")
        return congresspeople

if __name__ == '__main__':
    api = CongressPeople()
    congresspeople = api.mine()