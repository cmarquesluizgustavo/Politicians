from src.miners.base import BaseMiner
import asyncio
import aiohttp
import pandas as pd


class AuthorsMiner(BaseMiner):
    def __init__(self, **kwargs):
        """
        Initialize the AuthorsMiner.

        Args:
            **kwargs: Additional arguments.
        """
        super().__init__(name='Authors', log_file='logs/authors.log', **kwargs)
        self.output_path = "data/authors/"
        self.years = list(range(2000, 2024))


    async def get_authors(self, year):
        download_link = "https://dadosabertos.camara.leg.br/arquivos/proposicoesAutores/csv/proposicoesAutores-{year}.csv"
        url = download_link.format(year=year)
        async with aiohttp.ClientSession() as session:
            response = await session.get(url)
            with open(f"{self.output_path}authors-{year}.csv", "wb") as f:
                f.write(await response.read())
            self.logger.info(f"Finished downloading authors for year {year}.")

    def create_dataframe(self):
        authors = pd.DataFrame()
        for year in self.years:
            author = pd.read_csv(f"{self.output_path}authors-{year}.csv", sep=";")
            author = author[['idProposicao', 'uriAutor', 'nomeAutor']]
            author['year'] = year
            authors = pd.concat([authors, author])

        authors = authors.dropna(subset=['uriAutor'])
        authors['type'] = authors['uriAutor'].apply(lambda x: x.split("/")[-2])
        authors['id'] = authors['uriAutor'].apply(lambda x: x.split("/")[-1])
        authors = authors.drop(columns=['uriAutor'])
        authors.to_csv(f"{self.output_path}authors.csv", index=False)
        self.authors = authors

    def mine(self):
        """
        Mine the API for authors.
        """
        tasks = []
        for year in self.years:
            tasks.append(self.get_authors(year))
        asyncio.run(asyncio.wait(tasks))
        self.create_dataframe()
        self.logger.info("Finished mining authors.")

if __name__ == "__main__":
    miner = AuthorsMiner()
    miner.mine()