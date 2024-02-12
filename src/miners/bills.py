"""
This module contains the BillsMiner class, 
which is responsible for mining the API for bills data.
"""

import asyncio
import aiohttp
import pandas as pd
from base import BaseMiner


class BillsMiner(BaseMiner):
    """
    Class responsible
    for mining the API for bills data.
    """

    def __init__(self, **kwargs):
        """
        Initialize the ProposalsMiner.

        Args:
            **kwargs: Additional arguments.
        """
        self.output_path = kwargs.get("output_path", "data/miners/proposals/")
        super().__init__(
            name="Proposals",
            log_file="logs/miners/proposals.log",
            output_path=self.output_path,
            **kwargs,
        )
        self.proposal_types = ["PL", "PEC", "PLN", "PLP", "PLV", "PLC"]
        self.years = list(range(2000, 2024))

    async def get_proposals(self, year: int):
        """
        Get proposals for a given year.
        """
        download_link = (
            "https://dadosabertos.camara.leg.br/"
            + "arquivos/proposicoes/csv/proposicoes-{year}.csv"
        )
        url = download_link.format(year=year)
        async with aiohttp.ClientSession() as session:
            response = await session.get(url)
            with open(f"{self.output_path}proposals-{year}.csv", "wb") as f:
                f.write(await response.read())
            self.logger.info("Finished downloading proposals for year %s.", year)

    def create_dataframe(self):
        """ "
        Create a dataframe with all the proposals.
        """
        proposals = pd.DataFrame()
        for year in self.years:
            try:
                proposal = pd.read_csv(
                    f"{self.output_path}proposals-{year}.csv", sep=";", low_memory=False
                )
            except Exception:
                self.logger.error("Could not find proposals for year %s.", year)
                continue
            proposal = proposal[
                [
                    "id",
                    "ultimoStatus_idSituacao",
                    "siglaTipo",
                    "ano",
                    "ementa",
                    "keywords",
                ]
            ]
            proposal = proposal[proposal["siglaTipo"].isin(self.proposal_types)]
            proposal["keywords"] = proposal["keywords"].str.replace("\n", " ")
            proposal["keywords"] = proposal["keywords"].str.replace("\r", " ")
            proposal["ultimoStatus_idSituacao"] = (
                proposal["ultimoStatus_idSituacao"].fillna(0.0).astype(int)
            )
            proposals = pd.concat([proposals, proposal])

        proposals.to_csv(f"{self.output_path}proposals.csv", index=False)

    async def run_tasks(self):
        """
        Run the tasks to get the proposals.
        """
        tasks = []
        for year in self.years:
            tasks.append(asyncio.create_task(self.get_proposals(year)))

        await asyncio.gather(*tasks)

    def mine(self):
        """
        Mine the API for proposals.
        """
        asyncio.run(self.run_tasks())
        self.create_dataframe()
        self.logger.info("Finished mining proposals.")


if __name__ == "__main__":
    miner = BillsMiner()
    miner.mine()
