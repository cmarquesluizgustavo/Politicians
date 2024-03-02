import pandas as pd
from pre_processing import pre_processing
from base import NetworkFactory

congresspeople = pd.read_csv("data/miners/enriched_congresspeople.csv")
proposals = pd.read_csv("data/miners/proposals/proposals.csv")
authorship = pd.read_csv("data/miners/authors/authors.csv")

authorship = authorship[authorship["type"] == "deputados"]
congresspeople = pre_processing(congresspeople)

features = [
    "education",
    "gender",
    "siglaUf",
    "siglaPartido",
    "region",
    "occupation",
    "ethnicity",
    "age_group",
]

factory = NetworkFactory(congresspeople, authorship, features, "data/network_builder/")
factory.create_networks()
