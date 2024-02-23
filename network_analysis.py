import pickle
from typing import Mapping
from networkx import jaccard_coefficient
import numpy as np
import pandas as pd
import networkx as nx


class BasicStatistics:
    """
    This class is a helper to get the basic statistics of a graph.
    """

    def __init__(self, G: nx.Graph) -> None:
        self.g = G
        self.number_of_nodes = G.number_of_nodes()
        self.number_of_edges = G.number_of_edges()
        self.connected_components = nx.number_connected_components(G)
        self.density = nx.density(G)

        self.degree_distribution = dict(G.degree())

        self.connected_components = nx.number_connected_components(G)
        self.largest_cc = self.g.subgraph(max(nx.connected_components(self.g), key=len))
        self.largest_cc_rel_size = (
            self.largest_cc.number_of_nodes() / self.number_of_nodes
        )

    def get_centrality_distributions(self) -> dict:
        """
        This function returns the centrality distributions of a graph.
        Distributions are pagerank, betweenness and closeness.
        """
        pagerank_distribution = nx.pagerank(self.g)
        betweenness_distribution = nx.betweenness_centrality(self.g)
        closeness_distribution = nx.closeness_centrality(self.g)

        return {
            "pagerank_distribution": pagerank_distribution,
            "betweenness_distribution": betweenness_distribution,
            "closeness_distribution": closeness_distribution,
        }

    def get_clustering(self) -> dict:
        """
        This function returns the clustering of a graph.
        """
        avg_clustering = nx.average_clustering(self.g)
        global_clustering = nx.transitivity(self.g)
        return {
            "avg_clustering": avg_clustering,
            "global_clustering": global_clustering,
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


class featuresGains:
    """
    This class is a wrapper to calculate the gain in similarity of each
    features in a graph. It uses the Jaccard coefficient to measure the
    influence of each features in the similarity of a node.
    """

    def __init__(self, G: nx.Graph) -> None:
        self.g = G

        self.nodes_jaccards = pd.DataFrame()
        base_jaccard = self.avg_jaccard_4_nodes_by_graph(self.g)
        # self.nodes_jaccards["Base"] = base_jaccard.values()
        # self.
        # self.gains = {}
        # self.avg_jaccard["all"] = self.avg_jaccard_4_nodes_by_graph(self.g)

    def get_subgraphs(self, feature: str) -> Mapping[str, nx.Graph]:
        """
        This function returns the subgraphs induced by a feature.
        """
        feature_values = set(nx.get_node_attributes(self.g, feature).values())
        feature_values_subgraphs = {}
        for feature_value in feature_values:
            feature_values_subgraphs[feature_value] = self.g.subgraph(
                [n for n, d in self.g.nodes(data=True) if d[feature] == feature_value]
            )
        return feature_values_subgraphs

    def get_avg_jaccard(self, node, subgraph: nx.Graph) -> float:
        """
        This function returns the average Jaccard coefficient of a node in a subgraph.
        """
        neighbors = set(subgraph.neighbors(node))
        if len(neighbors) == 0:
            return 0.0

        jaccard_coefficients = nx.jaccard_coefficient(
            subgraph, [(node, n) for n in neighbors]
        )

        avg_jaccard = 0
        for u, v, jaccard in jaccard_coefficients:
            avg_jaccard += jaccard
        return avg_jaccard / len(neighbors)

    def avg_jaccard_4_nodes_by_graph(self, subgraph: nx.Graph) -> dict[int, float]:
        """
        This function calculates the average Jaccard coefficient for each node in a subgraph.
        """
        avg_jaccard = {}
        for node in subgraph.nodes:
            avg_jaccard[node] = self.get_avg_jaccard(node, subgraph)
        return avg_jaccard

    def get_jaccard_4_feature(self, feature: str) -> dict:
        """
        This function returns the average Jaccard coefficient for each feature.
        """
        feature_values_subgraphs = self.get_subgraphs(feature)
        for feature_value, subgraph in feature_values_subgraphs.items():
            self.nodes_jaccards[feature_value] = self.avg_jaccard_4_nodes_by_graph(
                subgraph
            ).values()


self = featuresGains(G)


def CCDF(data: list[int | float]) -> np.ndarray:
    """
    This function returns the complementary
    cumulative distribution function of a list of data.
    """
    np_data = np.array(data)
    np_data.sort()
    s = np_data.sum()
    cdf = np_data.cumsum(0) / s
    ccdf = 1 - cdf
    return ccdf


if __name__ == "__main__":
    G = pickle.load(open("data/networks/2023.pkl", "rb"))
    getBasicStatistics(G)
