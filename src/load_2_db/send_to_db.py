import os
import pandas as pd
import dotenv
from sqlalchemy import column
from src.network_builder.pre_processing import pre_processing
from load_2_db.shippers import (
    connect_to_db,
    add_congresspeople_to_db,
    add_terms_to_db,
    add_bills_to_db,
    add_authorship_to_db,
    add_networks_to_db,
)

DATABASE_URL = dotenv.get_key(dotenv.find_dotenv(), "DATABASE_URL")
if DATABASE_URL is None:
    raise ValueError("DATABASE_URL not found in .env file.")

CONGRESSPEOPLE_PATH = "data/miners/enriched_congresspeople.csv"
PROPOSALS_PATH = "data/miners/proposals/proposals.csv"
AUTHORS_PATH = "data/miners/authors/authors.csv"

NETWORKS_PATH = "data/network_analyzer/networks/networks.csv"
NETWORKS_FEATURES_PATH = "data/network_analyzer/networks/networks_features.csv"
NODES_FEATURES_PATH = "data/network_analyzer/nodes/nodes_features.csv"

networks_df = pd.read_csv(NETWORKS_PATH)
congresspeople_df = pd.read_csv(CONGRESSPEOPLE_PATH)
proposals_df = pd.read_csv(PROPOSALS_PATH)
authors_df = pd.read_csv(AUTHORS_PATH)

congresspeople_df = pre_processing(congresspeople_df)
authors_df = authors_df[
    (authors_df["type"] == "deputados")
    & (authors_df["idProposicao"].isin(proposals_df["id"]))
    & (authors_df["id"].isin(congresspeople_df["id"]))
]


statistics_df = pd.DataFrame(
    columns=[
        "id",
        "type",
        "value",
        "label",
        "congressperson_statistics_id",
        "network_id",
    ]
)
# Convert networks_df to statistics_df
networks_df = networks_df.set_index("period")
networks_df = networks_df.drop(columns=["type"])
networks_df = networks_df.stack().reset_index()
networks_df.columns = ["network_id", "label", "value"]
networks_df["type"] = "network"
networks_df["congressperson_statistics_id"] = None

statistics_df = pd.concat([statistics_df, networks_df])


session = connect_to_db(DATABASE_URL)
add_congresspeople_to_db(congresspeople_df, session)
add_networks_to_db(networks_df[["period", "type"]].set_index("period"), session)
add_terms_to_db(congresspeople_df, session)
add_bills_to_db(proposals_df, session)
add_authorship_to_db(authors_df, session)


session.close()
