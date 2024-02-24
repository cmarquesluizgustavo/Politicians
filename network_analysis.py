import pickle
from re import sub
from typing import Mapping
import numpy as np
import pandas as pd
import networkx as nx
from matplotlib import pyplot as plt


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


def is_clique(G):
    """
    This function returns if a graph is a clique.
    """
    n = len(G)
    return G.number_of_edges() == n * (n - 1) / 2


class featuresGains:
    """
    This class is a wrapper to calculate the gain in similarity of each
    features in a graph. It uses a similarity coefficient to measure the
    influence of each features in the similarity of a node.
    """

    def __init__(
        self, G: nx.Graph, target_feature: list, similarity_algorithm: str = "jaccard"
    ):
        self.g = G
        self.target_feature = target_feature
        self.similarity_algorithm = similarity_algorithm

        self.similarity = self.get_all_similarities()

    def get_all_similarities(self) -> pd.DataFrame:
        base_similarity = self.avg_similarity_4_nodes_by_graph(self.g)

        similarity = pd.DataFrame(
            columns=pd.MultiIndex.from_tuples(
                [("Base", "value"), ("Base", "label"), ("Base", "gain")]
            )
        )
        similarity["Base", "value"] = pd.Series(base_similarity).to_frame("Base")
        similarity["Base", "label"] = "Base"
        similarity["Base", "gain"] = 0

        for feature in self.target_feature:
            feature_df = self.get_similarity_4_feature(feature)
            feature_df[feature, "gain"] = (
                feature_df[feature, "value"] - similarity[("Base", "value")]
            )
            similarity = pd.merge(
                similarity, feature_df, left_index=True, right_index=True
            )

        return similarity

    def get_subgraphs(self, feature: str) -> Mapping[str, nx.Graph]:
        """
        This function returns the subgraphs induced by a feature.
        """
        feature_values = set(nx.get_node_attributes(self.g, feature).values())
        feature_values_subgraphs = {}
        for feature_value in feature_values:
            feature_values_subgraphs[feature_value] = self.g.subgraph(
                [
                    n
                    for n, d in self.g.nodes(data=True)
                    if feature in d and d[feature] == feature_value
                ]
            )
        return feature_values_subgraphs

    def avg_similarity_4_nodes_by_graph(self, subgraph: nx.Graph) -> dict[int, float]:
        """
        This function calculates the average similarity of a node in a subgraph.
        """
        # If the subgraph is connected, the average similarity is 1
        if is_clique(subgraph):
            return {k: 1 for k in subgraph.nodes()}
        if self.similarity_algorithm == "jaccard":
            similarities = nx.jaccard_coefficient(subgraph)
        else:
            raise ValueError("Similarity algorithm not implemented.")
        avg_similarity, node_neighbors = {}, {}

        for u, v, similarity in similarities:
            if similarity != 0:
                avg_similarity[u] = avg_similarity.get(u, 0) + similarity
                avg_similarity[v] = avg_similarity.get(v, 0) + similarity
                node_neighbors[u] = node_neighbors.get(u, 0) + 1
                node_neighbors[v] = node_neighbors.get(v, 0) + 1

        avg_similarity = {
            node: avg_similarity[node] / node_neighbors[node] for node in avg_similarity
        }

        return avg_similarity

    def get_similarity_4_feature(self, feature: str) -> pd.DataFrame:
        """
        This function returns the average similarity coefficient for each feature value.
        """
        feature_values_subgraphs = self.get_subgraphs(feature)
        feature_similarity_df = pd.DataFrame()

        for feature_value, subgraph in feature_values_subgraphs.items():
            avg_similarity = self.avg_similarity_4_nodes_by_graph(subgraph)

            feature_similarity_df_inter = pd.DataFrame(
                {
                    feature: feature_value,
                    "value": list(avg_similarity.values()),
                },
                index=list(avg_similarity.keys()),
            )

            if feature_similarity_df_inter.empty:
                continue
            feature_similarity_df = pd.concat(
                [feature_similarity_df, feature_similarity_df_inter]
            )

        feature_similarity_df.columns = pd.MultiIndex.from_tuples(
            [(feature, "label"), (feature, "value")]
        )

        return feature_similarity_df


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
    self = featuresGains(
        G,
        target_feature=[
            "siglaPartido",
            "siglaUf",
        ],
    )
