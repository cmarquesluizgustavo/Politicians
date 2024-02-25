"""
This module contains the class BasicStatistics, which is 
used to get the most vanilla statistics of a network.
"""

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
            log_level=20,
            log_file="logs/network_analyzer/basic_statistics.log",
        )

        self.number_of_nodes = g.number_of_nodes()
        self.number_of_edges = g.number_of_edges()

        self.connected_components = nx.number_connected_components(g)
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

        self.diameter = self.get_diameter()

        self.logger.info("Getting centrality distributions")
        self.centrality_distributions = self.get_centrality_distributions()

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
