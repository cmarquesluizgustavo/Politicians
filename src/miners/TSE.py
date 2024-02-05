"""
Module to mine TSE reports.
"""

import os
import asyncio
import zipfile
import aiohttp
import pandas as pd
from base import BaseMiner


class TSEReportsMiner(BaseMiner):
    """
    Class to mine TSE reports.
    """

    def __init__(self, **kwargs):
        """
        Initialize the TSEReportsMiner.

        Args:
            **kwargs: Additional arguments.
        """
        self.output_path = kwargs.get("output_path", "data/tse/")
        self.years = kwargs.get("years", list(range(2000, 2024, 4)))
        super().__init__(
            name="TSEReports",
            log_file="logs/tse_reports.log",
            output_path=self.output_path,
            **kwargs,
        )

    async def get_candidates_zip(self, year: int):
        """
        Download the zip file for the candidates.
        """

        download_link = f"https://cdn.tse.jus.br/estatistica/sead/odsele/\
            consulta_cand/consulta_cand_{year}.zip"
        url = download_link.format(year=year)
        async with aiohttp.ClientSession() as session:
            response = await session.get(url)
            with open(f"{self.output_path}candidates-{year}.zip", "wb") as f:
                f.write(await response.read())
            self.logger.info("Finished downloading candidates for year %s.", year)

    def extract_zip(self, year: int):
        """
        Extract the zip file for the candidates.
        """
        with zipfile.ZipFile(
            f"{self.output_path}candidates-{year}.zip", "r"
        ) as zip_ref:
            zip_ref.extractall(f"{self.output_path}candidates-{year}/")

        os.remove(f"{self.output_path}candidates-{year}.zip")

    def create_dataframe(self):
        """ "
        Create a dataframe with all the candidates.
        """
        candidates = pd.DataFrame()
        for year in self.years:
            for file in os.listdir(f"{self.output_path}candidates-{year}/"):
                if not file.endswith("_BRASIL.csv"):
                    continue
                candidate = pd.read_csv(
                    f"{self.output_path}candidates-{year}/{file}",
                    sep=";",
                    encoding="latin-1",
                    low_memory=False,
                )
                candidate = candidate[candidate["DS_CARGO"] == "DEPUTADO FEDERAL"]
                candidate = candidate[
                    [
                        "ANO_ELEICAO",
                        "NM_UE",
                        "DS_CARGO",
                        "NM_CANDIDATO",
                        "SG_PARTIDO",
                        "DS_GENERO",
                        "DS_GRAU_INSTRUCAO",
                        "DS_ESTADO_CIVIL",
                        "DS_COR_RACA",
                        "DS_OCUPACAO",
                        "DS_SIT_TOT_TURNO",
                        "DT_NASCIMENTO",
                        "NR_CPF_CANDIDATO",
                    ]
                ]
                candidates = pd.concat([candidates, candidate])

        candidates.to_csv(f"{self.output_path}candidates.csv", index=False)
        self.logger.info("Created dataframe with all candidates.")

    async def run_tasks(self):
        """
        Run the tasks to download the candidates.
        """
        tasks = []
        for year in self.years:
            tasks.append(asyncio.create_task(self.get_candidates_zip(year)))

        await asyncio.gather(*tasks)

    def mine(self):
        """
        Mine the API for candidates.
        """
        self.logger.info("Started mining candidates.")
        asyncio.run(self.run_tasks())

        for year in self.years:
            self.extract_zip(year)
        self.create_dataframe()
        self.logger.info("Finished mining candidates.")


if __name__ == "__main__":
    miner = TSEReportsMiner()
    miner.mine()
