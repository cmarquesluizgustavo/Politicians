import os
import pickle
import logging
from datetime import datetime
import pandas as pd
import networkx as nx


class NetworkFactory:
    """
    Factory class to create the network
    """

    def create_network(
        self,
        congresspeople: pd.DataFrame,
        authors_dict: dict[int, list[str]],
        features: list[str],
    ):
        """
        Create a network for each period
        Args:
            congresspeople (pd.DataFrame): DataFrame with congresspeople data
            authors_dict (dict[int, list[str]]): Dictionary with proposal
                                        as key and list of authors as value
            features (list[str]): List of features to add to the graph
        """
        for year in authors_dict.keys():
            id_legislatura = (year - 1999) // 4 + 51
            congress = congresspeople[congresspeople["idLegislatura"] == id_legislatura]

            NetworkBuilder(
                congress,
                {year: authors_dict[year]},
                features,
                str(year),
            )

        for id_legislatura in congresspeople["idLegislatura"].unique():
            election_year = {
                57: 2022,
                56: 2018,
                55: 2014,
                54: 2010,
                53: 2006,
                52: 2002,
                51: 1998,
            }
            congress = congresspeople[congresspeople["idLegislatura"] == id_legislatura]

            years = list(range(election_year[55] + 1, election_year[55] + 5))
            authors_dict_term = {}
            for year in years:
                authors_dict_term.update({year: authors_dict[year]})

            NetworkBuilder(congress, authors_dict_term, features, str(id_legislatura))


class NetworkBuilder:
    """
    This class builds the network from the data.
    It starts with the congresspeople as nodes. Then we add edges between congresspeople
    who have co-authored a proposal. The weight of the edge is the number of proposals
    We also have other attributes for the edges, such as party affiliation, race, etc.
    """

    def __init__(
        self,
        congresspeople: pd.DataFrame,
        authors_dict: dict[int, list[str]],
        features: list[str],
        period: str,
    ):
        """
        Args:
            congresspeople (pd.DataFrame): DataFrame with congresspeople data
            authors_dict (dict[int, list[str]]): Dictionary with proposal
                                                 as key and list of authors as value
            features (list[str]): List of features to add to the graph
            period (str): Period of the congress
        """
        self.g = nx.Graph()
        self.congresspeople = congresspeople
        self.authors_dict = authors_dict
        self.features = features
        self.period = period

        self.logger = NetworkLogger(
            "NetworkBuilder",
            logging.INFO,
            f"logs/networks/network_builder_{period}.log",
        )

        # self.add_nodes(congresspeople)
        # for feature in features:
        #     print(feature)
        #     relations = self.createRelation(congresspeople, feature)
        #     self.g.add_edges_from(relations, key=feature)
        self.add_nodes_attributes()
        self.add_edges_proposals()
        self.save_graph(f"data/networks/{period}_network.gpickle")

    def save_graph(self, path: str):
        """
        Save the graph in a pickle file
        """
        with open(path, "wb") as f:
            pickle.dump(self.g, f)

    def create_relation(self, target_column: str):
        """
        Create relations between congresspeople based on a target column.
        Args:
            target_column (str): Column to create relations from
        """
        relations = []
        for relation in self.congresspeople[target_column].unique():
            target_df = self.congresspeople[
                self.congresspeople[target_column] == relation
            ]
            for index, row in target_df.iterrows():
                for index, row in target_df.loc[index + 1 :].iterrows():
                    relations.append((row["id"], target_df.iloc[0]["id"]))
        return relations

    def add_nodes(self):
        """'
        Add nodes to the graph (without attributes)
        Args:
            congresspeople (pd.DataFrame): DataFrame with congresspeople data
        """
        for _, data in self.congresspeople.iterrows():
            self.g.add_node(data["id"])

    def add_nodes_attributes(self):
        """
        Add nodes to the graph with attributes
        """
        for _, data in self.congresspeople.iterrows():
            self.g.add_node(data["id"], **data[self.features].to_dict())

    def add_edges_proposals(self):
        """
        Add edges to the graph
        """
        for proposal in self.authors_dict:
            authors = self.authors_dict[proposal]
            coauthors_len = len(self.authors_dict[proposal])
            for i in range(coauthors_len):
                for j in range(i + 1, coauthors_len):
                    if not self.g.has_edge(authors[i], authors[j]):
                        self.g.add_edge(authors[i], authors[j], weight=1)
                    else:
                        self.g[authors[i]][authors[j]]["weight"] += 1


class NetworkLogger(logging.Logger):
    """
    Base class for loggers. Inherits from the logging.Logger class.
    Deals with logging configurations.
    """

    def __init__(self, name, log_level: int, log_file: str, **kwargs):
        """
        Initialize the BaseLogger.
        """
        super().__init__(kwargs.get("logger", "NetworkLogger"))
        self.handle_logging(name, log_level, log_file, kwargs.get("terminal", False))

    def handle_logging(self, name, log_level, log_file, terminal) -> None:
        """
        Set up logging configurations.
        """
        self.log_level = log_level
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        parts = log_file.split("/")
        parts[-1] = f"%s-%s" % (datetime.now().strftime("%Y-%m-%d@%H:%M:%S"), parts[-1])
        self.log_file = "/".join(parts)
        self.setLevel(self.log_level)
        self.addHandler(logging.FileHandler(self.log_file))
        if terminal:
            self.addHandler(logging.StreamHandler())
        self.info(
            "Initializing logger %s - Log level: %s - Log file: %s @ %s",
            name,
            logging.getLevelName(self.log_level),
            self.log_file,
            datetime.now().strftime("%Y-%m-%d@%H:%M:%S"),
        )
