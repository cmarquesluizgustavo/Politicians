import pandas as pd
from pre_processing import pre_processing
from base import NetworkFactory

congresspeople = pd.read_csv("data/miners/enriched_congresspeople.csv")
proposals = pd.read_csv("data/miners/proposals/proposals.csv")
authors = pd.read_csv("data/miners/authors/authors.csv")

authors = authors[authors["type"] == "deputados"]
congresspeople = pre_processing(congresspeople)
congresspeople = congresspeople[
    [
        "id",
        "idLegislatura",
        "election_year",
        "nomeEleitoral",
        "education",
        "gender",
        "siglaUf",
        "siglaPartido",
        "region",
        "occupation",
        "ethnicity",
        "age_group",
    ]
]

# Create authors dict. It must be an empty list for each proposal then we append the authors. One per year
authors_dict = {}

for year in range(2000, 2024):
    authors_dict[year] = {}
    for index, row in authors[authors["year"] == year].iterrows():
        if row["idProposicao"] in authors_dict[year]:
            authors_dict[year][row["idProposicao"]].append(row["id"])
        else:
            authors_dict[year][row["idProposicao"]] = [row["id"]]

    # remake the dict and get only the proposals with more than one author and less than 10
    # authors_dict[year] = {k: v for k, v in authors_dict[year].items() if len(v) > 1 and len(v) < 10}

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

factory = NetworkFactory(
    congresspeople, authors_dict, features, "data/network_builder/"
)
factory.create_networks()
