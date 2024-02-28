from typing import Mapping
import pandas as pd
import networkx as nx
from base_logger import NetworkAnalyzerLogger


class SimilarityStatistics:
    """
    This class is a wrapper to calculate the gain in similarity of each
    features in a graph. It uses a similarity coefficient to measure the
    influence of each features in the similarity of a node.
    """

    def __init__(self, g: nx.Graph, target_features: list, similarity_algorithm: str):
        self.g = g
        self.target_features = target_features
        self.similarity_algorithm = similarity_algorithm

        self.logger = NetworkAnalyzerLogger(
            name=g.name,
            client_class="SimilarityStatistics",
            log_level=20,
            log_file=f"logs/network_analyzer/similarity_statistics/{g.name}_{similarity_algorithm}.log",
        )

        self.logger.info(
            "Calculating similarity statistics. Algorithm: %s", similarity_algorithm
        )
        self.similarity = self.get_all_similarities()

    def get_all_similarities(self) -> pd.DataFrame:
        """
        This function returns the similarity coefficient for each feature.
        """
        base_similarity = self.avg_similarity_4_nodes_by_graph(self.g)

        similarity = pd.DataFrame(
            columns=pd.MultiIndex.from_tuples(
                [("Base", "value"), ("Base", "label"), ("Base", "gain")]
            )
        )
        similarity["Base", "label"] = "Base"
        similarity["Base", "value"] = pd.Series(base_similarity).to_frame("Base")
        similarity["Base", "gain"] = 0

        for feature in self.target_features:
            self.logger.info("Calculating similarity for feature %s", feature)
            feature_df = self.get_similarity_4_feature(feature)
            feature_df[feature, "gain"] = (
                feature_df[feature, "value"] - similarity[("Base", "value")]
            ) / similarity[("Base", "value")]
            similarity = pd.merge(
                similarity, feature_df, left_index=True, right_index=True, how="outer"
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
        if self.is_clique(subgraph):
            return {k: 1 for k in subgraph.nodes()}
        if self.similarity_algorithm == "jaccard":
            similarities = nx.jaccard_coefficient(subgraph)
        elif self.similarity_algorithm == "adamic_adar":
            similarities = nx.adamic_adar_index(subgraph)
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

    @staticmethod
    def is_clique(g):
        """
        This function returns if a graph is a clique.
        """
        n = len(g)
        return g.number_of_edges() == n * (n - 1) / 2


class SimilarityAndGainsStatistics(SimilarityStatistics):
    """
    This class calls the basicStatistics and featuresGains classes to get the results.
    The specific for the whole network, or to a specific subgraph or a node
    """

    def __init__(self, g: nx.Graph, target_features: list, similarity_algorithm: str):
        super().__init__(g, target_features, similarity_algorithm)

        self.logger.info("Calculating gains by feature")
        self.gains_by_feature = {}
        for feature in self.target_features:
            self.gains_by_feature[feature] = self.get_gains_by_feature(feature)

        self.logger.info("Calculating gains by node")
        self.gains_by_node = pd.concat(
            [self.get_gains_by_node(node) for node in self.g.nodes()]
        ).set_index("node_id")

        self.logger.info("Gains calculated")

    def get_gains_by_feature(self, feature: str) -> pd.DataFrame:
        """'
        Calculate the gain of an specific feature
        """
        feature_gain = {}
        for feature_label in self.similarity[feature]["label"].unique():
            feature_gain[feature_label] = self.similarity[
                self.similarity[feature]["label"] == feature_label
            ][feature]["gain"].mean()
        feature_gain["global"] = self.similarity[feature]["gain"].mean()
        feature_gain["period"] = self.g.name
        feature_gain = pd.DataFrame(feature_gain, index=[self.g.name])
        return feature_gain

    def get_gains_by_node(self, node: str) -> pd.DataFrame:
        """'
        Calculate the gains of an specific node for each feature
        """
        # Returns empty dataframe if node not in the list of gains
        if node not in self.similarity.index:
            return pd.DataFrame()
        node_gain = self.similarity[self.similarity.index == node]
        # Get only the gains of the target features
        node_gain = node_gain[[(feature, "gain") for feature in self.target_features]]
        node_gain.columns = node_gain.columns.droplevel(1)
        node_gain["period"] = self.g.name
        node_gain["node_id"] = node

        return pd.DataFrame(node_gain, index=[node])
