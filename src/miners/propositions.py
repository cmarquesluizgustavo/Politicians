from .base import BaseMiner
import asyncio
import aiohttp


class ProposalsMiner(BaseMiner):
    def __init__(self, **kwargs):
        """
        Initialize the ProposalsMiner.

        Args:
            **kwargs: Additional arguments.
        """
        super().__init__(name='Proposals', log_file='logs/proposals.log', **kwargs)
        self.base_url = 'https://dadosabertos.camara.leg.br/api/v2/'
        self.endpoint = 'proposicoes'

    async def get_proposals(self) -> dict:
        target_proposals = ["PL", "PEC", "PLN", "PLP", "PLV", "PLC"]
        target_proposals = ",".join(target_proposals)
        years = [year for year in range(2000, 2024)]
        years = ",".join([str(year) for year in years])
        params = {
            'itens': 1000,
            'ordem': 'ASC',
            'ordenarPor': 'id',
            'siglaTipo': target_proposals,
            'ano': years,
        }
        headers = {
            'accept': 'application/json',
        }
        url = f'{self.base_url}{self.endpoint}?itens=1000&ordem=ASC&ordenarPor=id&siglaTipo=PL,PEC,PLN,PLP,PLV,PLC&ano=2000&ano=2001&ano=2002&ano=2003&ano=2004&ano=2005&ano=2006&ano=2007&ano=2008&ano=2009&ano=2010&ano=2011&ano=2012&ano=2013&ano=2014&ano=2015&ano=2016&ano=2017&ano=2018&ano=2019&ano=2020&ano=2021&ano=2022&ano=2023'
        async with aiohttp.ClientSession() as session:
            data = await self.fetch_all_pages(url, headers, {}, session)

        return data

    def mine(self) -> None:
        """
        Mine the API.
        """
        pass