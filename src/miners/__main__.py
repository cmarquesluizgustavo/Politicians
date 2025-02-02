"""
Main module to run the miners. This module will run 
the miners and process the data to enrich the congresspeople data.
It uses threading to run the miners in parallel.
"""

from re import sub
import sys
import threading
import numpy as np
import pandas as pd
from congresspeople import CongressPeople
from authors import AuthorsMiner
from bills import BillsMiner
from TSE import TSEReportsMiner
from photos import CongressPeoplePhotos

congresspeople = CongressPeople()
authors = AuthorsMiner()
proposals = BillsMiner()
tse = TSEReportsMiner()
photos = CongressPeoplePhotos()


def process_congresspeople():
    """
    This process the data from Congress and TSE to enrich the congresspeople data
    """
    # Read in the data
    candidates = pd.read_csv("data/miners/tse/candidates.csv", encoding="utf-8")
    congresspeople_df = pd.read_csv(
        "data/miners/congresspeople/congresspeople.csv", encoding="utf-8"
    )

    # Filter out the candidates that are not congresspeople
    candidates = candidates[candidates["DS_CARGO"] == "DEPUTADO FEDERAL"]

    # Replace #NE with np.nan
    candidates["DS_COR_RACA"] = candidates["DS_COR_RACA"].replace("#NE", np.nan)
    candidates["DS_COR_RACA"] = candidates["DS_COR_RACA"].replace("NA", np.nan)
    candidates["DS_COR_RACA"] = candidates["DS_COR_RACA"].replace("#NE#", np.nan)

    # For the candidates with np.nan, look for the next ANO_ELEICAO and use that
    candidates["DS_COR_RACA"] = candidates.groupby("NM_CANDIDATO")[
        "DS_COR_RACA"
    ].bfill()

    # Filtering important columns
    candidates = candidates[
        [
            "ANO_ELEICAO",
            "DS_OCUPACAO",
            "NR_CPF_CANDIDATO",
            "DS_GENERO",
            "DS_GRAU_INSTRUCAO",
            "DS_ESTADO_CIVIL",
            "DS_COR_RACA",
        ]
    ]

    # Rename columns
    candidates.rename(
        {
            "NR_CPF_CANDIDATO": "cpf",
            "DS_OCUPACAO": "occupation",
            "DS_GENERO": "gender",
            "DS_GRAU_INSTRUCAO": "education",
            "DS_ESTADO_CIVIL": "marital_status",
            "DS_COR_RACA": "ethnicity",
            "ANO_ELEICAO": "election_year",
        },
        axis=1,
        inplace=True,
    )

    candidates["election_year"] = candidates["election_year"].astype("int64")

    # Adding election year to congresspeople and converting cpf to string
    election_year = {
        57: 2022,
        56: 2018,
        55: 2014,
        54: 2010,
        53: 2006,
        52: 2002,
        51: 1998,
    }
    congresspeople_df["election_year"] = congresspeople_df["idLegislatura"].apply(
        lambda x: election_year[x]
    )
    congresspeople_df["cpf"] = congresspeople_df["cpf"].astype(str)

    # Drop duplicated congresspeople, keeping the first one.
    # Each congressperson will have only one candidate per election year
    congresspeople_df = congresspeople_df.drop_duplicates(
        subset=["cpf", "election_year"], keep="first"
    )

    # Merge the data, by year and cpf
    congresspeople_df = congresspeople_df.merge(
        candidates, on=["cpf", "election_year"], how="left"
    )

    congresspeople_df.to_csv(
        "data/miners/enriched_congresspeople.csv", index=False, encoding="utf-8"
    )


def mine(sub_methods: list[str], target_years: list[int] = None):
    """
    This function runs the miners in parallel using threading.
    Args:
        sub_methods (list[str]): List of miners to run.
        target_years (list[int]): List of target years to pass to proposals and authors.
    """
    threads = []

    # Create threads for each miner
    congresspeople_thread = threading.Thread(target=congresspeople.mine)
    authors_thread = threading.Thread(
        target=authors.mine, kwargs={"target_years": target_years}
    )
    proposals_thread = threading.Thread(
        target=proposals.mine, kwargs={"target_years": target_years}
    )
    tse_thread = threading.Thread(target=tse.mine)
    photos_thread = threading.Thread(target=photos.mine)

    # Add threads to the list based on the sub_methods
    if sub_methods:
        if "congresspeople" in sub_methods:
            threads.append(congresspeople_thread)
            threads.append(tse_thread)
        if "proposals" in sub_methods:
            threads.append(proposals_thread)
            threads.append(authors_thread)
        if "photos" in sub_methods:
            threads.append(photos_thread)
    else:
        # Default behavior: run all miners
        threads.append(congresspeople_thread)
        threads.append(authors_thread)
        threads.append(proposals_thread)
        threads.append(tse_thread)
        threads.append(photos_thread)

    # Start all threads
    for thread in threads:
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()


def process_target_years(years_str):
    """Parses the target years from a comma-separated string into a list of integers."""
    try:
        return [int(year.strip()) for year in years_str.split(",")]
    except ValueError:
        print("Error: Target years must be a comma-separated list of integers.")
        sys.exit(1)


if __name__ == "__main__":
    # Extract sub-methods from command-line arguments
    SUB_METHODS = [
        sub(r"^--", "", arg)
        for arg in sys.argv[1:]
        if arg.startswith("--") and "=" not in arg
    ]

    # Extract target years if provided
    target_years_arg = next(
        (arg for arg in sys.argv[1:] if arg.startswith("--years=")), None
    )
    TARGET_YEARS = list(range(1999, 2025))
    if target_years_arg:
        TARGET_YEARS = process_target_years(target_years_arg.split("=", 1)[1])

    # Validate and print chosen parameters
    for arg in SUB_METHODS:
        if arg not in ["congresspeople", "proposals", "photos"]:
            print(f"Invalid argument: {arg}")
            sys.exit(1)
        else:
            print(f"Running {arg} miner")

    # Handle case where no sub-methods are specified
    if not SUB_METHODS:
        print(
            """
        If you want to choose specific miners, use the following arguments:
        --congresspeople: Run congresspeople and TSE miners
        --proposals: Run proposals and authors miners
        --photos: Run photos miner
        If no arguments are passed, all miners will be run.
    """
        )
        SUB_METHODS = ["congresspeople", "proposals", "photos"]

    # Print chosen years if provided
    if TARGET_YEARS:
        print(f"Target years: {', '.join(map(str, TARGET_YEARS))}")
    else:
        print("No target years specified. Using default behavior.")

    # Call main mining logic
    mine(SUB_METHODS, TARGET_YEARS)

    if "congresspeople" in SUB_METHODS:
        process_congresspeople()
