from src.miners.base import BaseMiner
import asyncio
import aiohttp
import pandas as pd


class ProposalsMiner(BaseMiner):
    def __init__(self, **kwargs):
        """
        Initialize the ProposalsMiner.

        Args:
            **kwargs: Additional arguments.
        """
        super().__init__(name='Proposals', log_file='logs/proposals.log', **kwargs)
        self.proposal_types = ["PL", "PEC", "PLN", "PLP", "PLV", "PLC"]
        self.output_path = "data/proposals/"
        self.years = list(range(2000, 2024))

    async def get_proposals(self, year):
        download_link = "https://dadosabertos.camara.leg.br/arquivos/proposicoes/csv/proposicoes-{year}.csv"
        url = download_link.format(year=year)
        async with aiohttp.ClientSession() as session:
            response = await session.get(url)
            with open(f"{self.output_path}proposals-{year}.csv", "wb") as f:
                f.write(await response.read())
            self.logger.info(f"Finished downloading proposals for year {year}.")

    def create_dataframe(self):
        proposals = pd.DataFrame()
        for year in self.years:
            proposal = pd.read_csv(f"{self.output_path}proposals-{year}.csv", sep=";")                   
            proposal = proposal[['id', 'ultimoStatus_idSituacao', 'siglaTipo', 'ano', 'ementa', 'keywords']]
            proposal = proposal[proposal['siglaTipo'].isin(self.proposal_types)]
            proposal['keywords'] = proposal['keywords'].str.replace("\n", " ")
            proposal['keywords'] = proposal['keywords'].str.replace("\r", " ")
            proposal['ultimoStatus_idSituacao'] = proposal['ultimoStatus_idSituacao'].fillna(0.0).astype(int)
            proposals = pd.concat([proposals, proposal])

        proposals.to_csv(f"{self.output_path}proposals.csv", index=False)
        self.proposals = proposals

    def mine(self):
        """
        Mine the API for proposals.
        """
        tasks = []
        for year in self.years:
            tasks.append(self.get_proposals(year))
        asyncio.run(asyncio.wait(tasks))
        self.create_dataframe()
        self.logger.info("Finished mining proposals.")

if __name__ == "__main__":
    miner = ProposalsMiner()
    miner.mine()