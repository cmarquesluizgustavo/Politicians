import os
import pickle
import logging
import threading
from datetime import datetime
import pandas as pd
import networkx as nx


class NetworkFactory:
    """
    Factory class to create the network
    """

    def __init__(
        self,
        congresspeople: pd.DataFrame,
        authorship: pd.DataFrame,
        features: list[str],
        path_to_save: str,
    ):
        """
        Args:
            congresspeople (pd.DataFrame): DataFrame with congresspeople data
            authorship (pd.DataFrame): DataFrame with proposals data, containing the authors and the year
            features (list[str]): List of features to add to the graph
        """
        self.congresspeople = congresspeople
        self.authorship = authorship
        self.features = features
        self.path_to_save = path_to_save
        os.makedirs(self.path_to_save, exist_ok=True)

    def _create_network(self, congress, authorship, period, path):
        NetworkBuilder(congress, authorship, self.features, str(period), path)

    def create_networks(self):
        """
        Create a network for each period
        """
        threads = []

        for year in self.authorship["year"].unique():
            id_legislatura = (year - 1999) // 4 + 51
            congress = self.congresspeople[
                self.congresspeople["idLegislatura"] == id_legislatura
            ]
            authorship = self.authorship[self.authorship["year"] == year]
            path = f"{self.path_to_save}/{year}.pkl"

            thread = threading.Thread(
                target=self._create_network,
                args=(congress, authorship, str(year), path),
            )
            threads.append(thread)
            thread.start()

        for id_legislatura in self.congresspeople["idLegislatura"].unique():
            election_year = {
                57: 2022,
                56: 2018,
                55: 2014,
                54: 2010,
                53: 2006,
                52: 2002,
                51: 1998,
            }
            congress = self.congresspeople[
                self.congresspeople["idLegislatura"] == id_legislatura
            ]
            years = list(
                range(
                    election_year[id_legislatura] + 1, election_year[id_legislatura] + 5
                )
            )
            authorship = self.authorship[self.authorship["year"].isin(years)]

            path = f"{self.path_to_save}/{id_legislatura}.pkl"

            thread = threading.Thread(
                target=self._create_network,
                args=(congress, authorship, str(id_legislatura), path),
            )
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()


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
        authorship: pd.DataFrame,
        features: list[str],
        period: str,
        path: str,
    ):
        """
        Args:
            congresspeople (pd.DataFrame): DataFrame with congresspeople data
            authorship: DataFrame with proposals data, containing the authors and the year
            features (list[str]): List of features to add to the graph
            period (str): Period of the congress
        """
        self.g = nx.Graph()
        self.g.name = period
        self.congresspeople = congresspeople
        self.authorship = authorship
        self.features = features
        self.period = period
        self.path = path

        self.logger = NetworkLogger(
            "NetworkBuilder",
            logging.INFO,
            f"logs/network_builder/network_builder_{self.period}.log",
        )

        self.add_nodes_attributes()
        self.logger.info("Nodes added")
        self.add_edges_proposals()
        self.logger.info("Edges added")
        self.save_graph()
        self.logger.info("Graph saved")

    def save_graph(self):
        """
        Save the graph in a pickle file
        """
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "wb") as f:
            pickle.dump(self.g, f)

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
        for proposal in self.authorship["idProposicao"].unique():
            authors = self.authorship[self.authorship["idProposicao"] == proposal][
                "id"
            ].to_list()
            coauthors_len = len(authors)
            if 2 > coauthors_len < 10:
                continue
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
            "Initializing logger %s - Log level: %s - Log file: %s",
            name,
            logging.getLevelName(self.log_level),
            self.log_file,
        )
