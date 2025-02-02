"""
Authors Miner.
"""

import os
import asyncio
import aiohttp
import pandas as pd
from base import BaseMiner


class AuthorsMiner(BaseMiner):
    """
    AuthorsMiner class.
    """

    def __init__(self, **kwargs):
        """
        Initialize the AuthorsMiner.

        Args:
            **kwargs: Additional arguments.
        """
        self.output_path = kwargs.get("output_path", "data/miners/authors/")
        super().__init__(
            name="Authors",
            log_file="logs/miners/authors.log",
            output_path=self.output_path,
            **kwargs,
        )

    async def get_authors(self, year, retry=0):
        """
        Get authors for a given year.
        """
        download_link = (
            "https://dadosabertos.camara.leg.br/arquivos/"
            + "proposicoesAutores/csv/proposicoesAutores-{year}.csv"
        )
        url = download_link.format(year=year)
        async with aiohttp.ClientSession() as session:
            response = await session.get(url)
            with open(f"{self.output_path}{year}.csv", "wb") as f:
                f.write(await response.read())

        if os.path.getsize(f"{self.output_path}{year}.csv") == 0:
            if retry > 2:
                raise ValueError(
                    f"File for year {year} was empty.\
                                 Rerun failed."
                )
            self.logger.info(
                "File for year %s was empty.\
                 Retrying for the %s time.",
                year,
                retry + 1,
            )
            await self.get_authors(year, retry=retry + 1)

        if not retry:
            self.logger.info("Finished downloading authors for year %s.", year)

    def create_dataframe(self):
        """
        Create a dataframe with all authors.
        """
        authors = pd.DataFrame()
        for year in self.years:
            author = pd.read_csv(
                f"{self.output_path}{year}.csv", sep=";", low_memory=False
            )
            author = author[["idProposicao", "uriAutor", "nomeAutor", "proponente"]]
            author["year"] = year
            authors = pd.concat([authors, author])
            
        authors = authors[authors["proponente"] == 1]
        authors = authors.dropna(subset=["uriAutor"])
        authors["type"] = authors["uriAutor"].apply(lambda x: x.split("/")[-2])
        authors["id"] = authors["uriAutor"].apply(lambda x: x.split("/")[-1])
        authors = authors.drop(columns=["uriAutor"])

        authors.to_csv(f"{self.output_path}authors.csv", index=False)
        self.logger.info("Finished creating authors dataframe.")

    async def run_tasks(self):
        """
        Run the tasks.
        """
        tasks = []
        for year in self.years:
            tasks.append(asyncio.create_task(self.get_authors(year)))
        await asyncio.gather(*tasks)

    def mine(self, target_years):
        """
        Mine the API for authors.
        """
        self.years = target_years
        asyncio.run(self.run_tasks())
        self.create_dataframe()
        self.logger.info("Finished mining authors.")


if __name__ == "__main__":
    miner = AuthorsMiner()
    miner.mine(target_years=list(range(1999, 2024)))
