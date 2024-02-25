"""
This module contains the class BasicStatistics, which is 
used to get the most vanilla statistics of a network.
"""

import pandas as pd
import networkx as nx
from base_logger import NetworkAnalyzerLogger


class BasicStatistics:
    """
    This class gets the basic statistics of a network.
    - Number of nodes and edges (with degree distribution)
    - Density and clustering (global and average)
    - Connected components and relative size of the largest connected component
    - Diameter
    - Centrality distributions (pagerank, betweenness and closeness)

    """

    def __init__(self, g: nx.Graph) -> None:
        self.g = g
        self.logger = NetworkAnalyzerLogger(
            name=g.name,
            client_class="BasicStatistics",
            log_level=20,
            log_file="logs/network_analyzer/basic_statistics.log",
        )

        self.number_of_nodes = g.number_of_nodes()
        self.number_of_edges = g.number_of_edges()

        self.density = nx.density(g)

        self.logger.info("Getting degree distribution")
        self.degree_distribution = dict(g.degree())

        self.logger.info("Getting connected components")
        self.connected_components = nx.number_connected_components(g)
        self.largest_cc = self.g.subgraph(max(nx.connected_components(self.g), key=len))
        self.largest_cc_rel_size = (
            self.largest_cc.number_of_nodes() / self.number_of_nodes
        )

        self.logger.info("Getting clustering coefficients")
        self.global_clustering = nx.transitivity(self.g)
        self.avg_clustering = nx.average_clustering(self.g)

        self.logger.info("Getting diameter")
        self.diameter = self.get_diameter()

        self.logger.info("Getting centrality distributions")
        self.centrality_distributions = self.get_centrality_distributions()

        self.logger.info("Basic statistics calculated")

    def get_centrality_distributions(self) -> dict:
        """
        This function returns the centrality distributions of a graph.
        Distributions are pagerank, betweenness and closeness.
        """
        pagerank_distribution = nx.pagerank(self.g, weight="weight")
        betweenness_distribution = nx.betweenness_centrality(self.g, weight="weight")
        # convert weights to 1/weight then normalize
        for u, v, d in self.g.edges(data=True):
            d["distance"] = 1 / d["weight"]
        closeness_distribution = nx.closeness_centrality(self.g, distance="distance")

        return {
            "pagerank_distribution": pagerank_distribution,
            "betweenness_distribution": betweenness_distribution,
            "closeness_distribution": closeness_distribution,
        }

    def get_diameter(self) -> int:
        """
        This function returns the diameter of a graph.
        If it's not connected, use the diameter of the largest cc.
        """
        if self.connected_components == 1:
            return nx.diameter(self.g)
        else:
            return nx.diameter(self.g.subgraph(self.largest_cc))

    def network_to_dataframe(self) -> pd.DataFrame:
        """
        This function returns the basic statistics as a dataframe.
        """
        return pd.DataFrame(
            {
                "number_of_nodes": [self.number_of_nodes],
                "number_of_edges": [self.number_of_edges],
                "density": [self.density],
                "connected_components": [self.connected_components],
                "largest_cc_rel_size": [self.largest_cc_rel_size],
                "global_clustering": [self.global_clustering],
                "avg_clustering": [self.avg_clustering],
                "diameter": [self.diameter],
            },
            index=[self.g.name],
        )

    def nodes_to_dataframe(self) -> pd.DataFrame:
        """
        This function returns the degree and centrality distributions as a dataframe.
        Node id is the index.
        """
        degree_df = pd.DataFrame(
            self.degree_distribution.items(), columns=["node_id", "degree"]
        ).set_index("node_id")
        pagerank_df = pd.DataFrame(
            self.centrality_distributions["pagerank_distribution"].items(),
            columns=["node_id", "pagerank"],
        ).set_index("node_id")
        betweenness_df = pd.DataFrame(
            self.centrality_distributions["betweenness_distribution"].items(),
            columns=["node_id", "betweenness"],
        ).set_index("node_id")
        closeness_df = pd.DataFrame(
            self.centrality_distributions["closeness_distribution"].items(),
            columns=["node_id", "closeness"],
        ).set_index("node_id")
        node_df = pd.concat(
            [degree_df, pagerank_df, betweenness_df, closeness_df], join="outer", axis=1
        ).fillna(0)

        node_df.set_index("node_id", inplace=True)
        return node_df
