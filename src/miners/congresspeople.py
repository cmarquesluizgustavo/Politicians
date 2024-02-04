import asyncio
import logging
from aiohttp import ClientSession
import pandas as pd
from base import BaseMiner


class CongressPeople(BaseMiner):
    def __init__(self, **kwargs) -> None:
        """
        CongressAPI constructor.

        Args:
            concurrency_limit (int): Maximum number of concurrent requests.
            sleep_spacing (int): Time in seconds to sleep between requests.
            max_retries (int): Maximum number of retries for a failed request.
        """
        self.output_path = kwargs.get('output_path', "data/congresspeople/")
        concurrency = asyncio.Semaphore(5)
        super().__init__(name='Authors', log_file='logs/congresspeople.log', 
                         output_path=self.output_path, terminal=True, concurrency=concurrency)
        self.base_url = 'https://dadosabertos.camara.leg.br/api/v2/'
        
    async def get_congresspeople_basic_info(self) -> list[dict]:
        """
        Fetch congresspeople basic infos asynchronously.
        
        Returns:
            list: List of congresspeople IDs.
        """
        legislatures_ids = list(range(51, 58))
        async with ClientSession() as session:
            tasks = [self.fetch_all_pages(f'{self.base_url}deputados', headers={}, params={'idLegislatura': legislature_id}, session=session) for legislature_id in legislatures_ids]
            congresspeople_basic_info = await asyncio.gather(*tasks)

        congresspeople_basic_info = [congressperson for congresspeople in congresspeople_basic_info for congressperson in congresspeople]
        for congressperson in congresspeople_basic_info:
            if type(congressperson) == list:
                for c in congressperson:
                    if 'id' in c:
                        congresspeople_basic_info.append(c)
                congresspeople_basic_info.remove(congressperson)
        return congresspeople_basic_info

    async def get_all_congresspeople_details(self, congresspeople_basic_info: list) -> pd.DataFrame:
        """
        Fetch details of all congresspeople asynchronously.

        Args:
            congresspeople_basic_info (list): List of congresspeople basic info.

        Returns:
            pd.DataFrame: Dataframe containing the details of all congresspeople.
        """
        congresspeople_ids = [congressperson['id'] for congressperson in congresspeople_basic_info]
        congresspeople_ids = list(set(congresspeople_ids))
        congresspeople_details = await self.fetch_endpoint_list(f'{self.base_url}deputados', congresspeople_ids, 'dados')
        
        # Add total status to each congressperson
        logging.info(f"Adding total status to each congressperson. Total: {len(congresspeople_details)}")
        counter = 0
        result_congresspeople_details = pd.DataFrame()
        for congressperson_detail in congresspeople_details.values():
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
    
    def create_dataframe(self, congresspeople: pd.DataFrame) -> pd.DataFrame:
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

        Returns:
            pd.DataFrame: Dataframe containing the congresspeople.s
        """
        self.logger.info("Mining congresspeople data...")
        congresspeople_basic_info = asyncio.run(self.get_congresspeople_basic_info())
        self.logger.info("Mine congresspeople basic info...")

        congresspeople = asyncio.run(self.get_all_congresspeople_details(congresspeople_basic_info))
        congresspeople = self.create_dataframe(congresspeople)
        self.logger.info(f"Finished mining congresspeople data. {len(congresspeople)} congresspeople were mined.")
        return congresspeople

if __name__ == '__main__':
    api = CongressPeople()
    api.mine()