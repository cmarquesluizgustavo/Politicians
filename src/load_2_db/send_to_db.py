import os
import pandas as pd
import dotenv
from src.network_builder.pre_processing import pre_processing
from src.load_2_db.utils import (
    connect_to_db,
    add_congresspeople_to_db,
    add_terms_to_db,
    add_bills_to_db,
    add_authorship_to_db,
    add_networks_to_db,
)

DATABASE_URL = dotenv.get_key(dotenv.find_dotenv(), "DATABASE_URL")
CONGRESSPEOPLE_PATH = "data/miners/enriched_congresspeople.csv"
PROPOSALS_PATH = "data/miners/proposals/proposals.csv"
AUTHORS_PATH = "data/miners/authors/authors.csv"

congresspeople_df = pd.read_csv(CONGRESSPEOPLE_PATH)
proposals_df = pd.read_csv(PROPOSALS_PATH)
authors_df = pd.read_csv(AUTHORS_PATH)

congresspeople_df = pre_processing(congresspeople_df)
authors_df = authors_df[
    (authors_df["type"] == "deputados")
    & (authors_df["idProposicao"].isin(proposals_df["id"]))
    & (authors_df["id"].isin(congresspeople_df["id"]))
]

session = connect_to_db(DATABASE_URL)

add_congresspeople_to_db(congresspeople_df, session)
add_terms_to_db(congresspeople_df, session)
add_bills_to_db(proposals_df, session)
add_authorship_to_db(authors_df, session)

networks_files = os.listdir("data/network_builder/")
networks = {
    file.split(".")[0]: "year" if len(file) == 8 else "term" for file in networks_files
}
add_networks_to_db(networks, session)

congressperson_statistics_df = pd.read_csv("data/miners/congressperson_statistics.csv")

session.close()
