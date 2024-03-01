import os
import pandas as pd
import dotenv
from src.network_builder.pre_processing import pre_processing
from src.load_2_db.shippers import (
    connect_to_db,
    add_congresspeople_to_db,
    add_terms_to_db,
    add_bills_to_db,
    add_authorship_to_db,
    add_networks_to_db,
    add_statistics_to_db,
)

DATABASE_URL = dotenv.get_key(dotenv.find_dotenv(), "DATABASE_URL")
if DATABASE_URL is None:
    raise ValueError("DATABASE_URL not found in .env file.")

CONGRESSPEOPLE_PATH = "data/miners/enriched_congresspeople.csv"
PROPOSALS_PATH = "data/miners/proposals/proposals.csv"
AUTHORS_PATH = "data/miners/authors/authors.csv"

NETWORKS_PATH = "data/network_analyzer/networks/networks.csv"
NETWORKS_FEATURES_PATH = "data/network_analyzer/networks/networks_features.csv"
FEATURES_PATH = "data/network_analyzer/features/"

features = [
    "age_group.csv",
    "gender.csv",
    "occupation.csv",
    "siglaPartido.csv",
    "education.csv",
    "region.csv",
    "siglaUf.csv",
]


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
networks_df.drop(columns=["Unnamed: 0"], inplace=True, errors="ignore")

# Getting the statistics from the features
features_networks_df = pd.DataFrame()
features_nodes_df = pd.DataFrame()
for algorithm in os.listdir(FEATURES_PATH):
    for feature in os.listdir(f"{FEATURES_PATH}{algorithm}"):
        if feature not in features:
            continue
        feature_df = pd.read_csv(f"{FEATURES_PATH}{algorithm}/{feature}")
        feature_df["type"] = algorithm
        features_networks_df = pd.concat([features_networks_df, feature_df])

    nodes_df = pd.read_csv(f"{FEATURES_PATH}{algorithm}/nodes.csv")
    nodes_df["type"] = algorithm
    features_nodes_df = pd.concat([features_nodes_df, nodes_df])

features_networks_df.drop(columns=["Unnamed: 0"], inplace=True, errors="ignore")
features_networks_df = features_networks_df.set_index(["period", "type"])
features_networks_df = features_networks_df.stack().reset_index()
features_networks_df.columns = ["network_id", "type", "label", "value"]
features_networks_df["congressperson_id"] = None

features_nodes_df = features_nodes_df.set_index(["period", "node_id", "type"])
features_nodes_df = features_nodes_df.stack().reset_index()
features_nodes_df.columns = [
    "network_id",
    "congressperson_id",
    "type",
    "label",
    "value",
]
features_nodes_df = features_nodes_df[
    features_nodes_df["congressperson_id"].isin(congresspeople_df["id"])
]

# Convert networks_df to statistics_df
networks_statistics_df = networks_df.set_index("period")
networks_statistics_df = networks_statistics_df.drop(columns=["type"])
networks_statistics_df = networks_statistics_df.stack().reset_index()
networks_statistics_df.columns = ["network_id", "label", "value"]
networks_statistics_df["type"] = "network"
networks_statistics_df["congressperson_id"] = None


statistics_df = pd.concat(
    [networks_statistics_df, features_networks_df, features_nodes_df]
)

session = connect_to_db(DATABASE_URL)
add_congresspeople_to_db(congresspeople_df, session)
add_networks_to_db(networks_df[["period", "type"]].drop_duplicates(), session)
add_terms_to_db(congresspeople_df, session)
add_bills_to_db(proposals_df, session)
add_authorship_to_db(authors_df, session)
add_statistics_to_db(statistics_df, session)

session.close()
