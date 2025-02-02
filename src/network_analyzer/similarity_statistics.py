from typing import Mapping
import pandas as pd
import networkx as nx
from collections import defaultdict
from base_logger import NetworkAnalyzerLogger
from similarity_algos import (
    jaccard_coefficient,
    adamic_adar_index,
    weighted_adamic_adar_index,
    weighted_jaccard_coefficient,
)


class SimilarityStatistics:
    """
    This class is a wrapper to calculate the gain in similarity of each
    features in a graph. It uses a similarity coefficient to measure the
    influence of each features in the similarity of a node.
    """

    def __init__(self, g: nx.Graph, target_features: list, similarity_algorithm: str):
        self.g = g
        self.target_features = target_features.copy()
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
        Computes similarity coefficients for each feature and the base graph.

        The function calculates the base similarity and the feature-wise similarities.
        It also computes a "gain" metric, which represents the relative difference in similarity
        compared to the base.

        Returns:
            pd.DataFrame: A DataFrame with similarity coefficients and gains for each feature.
        """
        # Compute base similarity
        base_similarity = self.avg_similarity_4_nodes_by_graph(self.g)

        # Initialize DataFrame for results
        similarity = pd.DataFrame(
            {
                ("Base", "value"): pd.Series(base_similarity),
                ("Base", "gain"): 0.0,
                ("Base", "label"): "Base",
            }
        )

        # Track features that have no similarity data
        skipped_features = []

        # Early return if no target features
        if not self.target_features:
            self.logger.info("No target features provided.")
            return similarity

        for feature in self.target_features:
            self.logger.info("Calculating similarity for feature: %s", feature)

            # Get similarity for the current feature
            feature_df = self.get_similarity_4_feature(feature)

            # Handle empty feature similarities
            if feature_df.empty:
                self.logger.info("Feature '%s' has no similarity. Skipping.", feature)
                skipped_features.append(feature)
                continue

            # Calculate gain as relative difference
            feature_df[(feature, "gain")] = (
                feature_df[(feature, "value")] - similarity[("Base", "value")]
            ).abs() / similarity[("Base", "value")]

            # Merge the feature similarity with the overall similarity DataFrame
            similarity = pd.concat([similarity, feature_df], axis=1)

        # Update the target features to exclude skipped ones
        self.target_features = [
            f for f in self.target_features if f not in skipped_features
        ]

        return similarity

    def get_subgraphs(self, feature: str) -> Mapping[str, nx.Graph]:
        """
        Extracts subgraphs induced by a specific feature's values.

        For each unique value of the given feature in the graph's node attributes,
        this function creates a subgraph consisting of nodes that share that feature value.

        Args:
            feature (str): The node attribute name to group nodes by.

        Returns:
            Mapping[str, nx.Graph]: A dictionary where keys are feature values, and values
            are the induced subgraphs for those feature values. If the feature is missing,
            an empty dictionary is returned.
        """
        # Extract feature values from node attributes
        node_attributes = nx.get_node_attributes(self.g, feature)

        # If the feature is not present, return an empty dictionary
        if not node_attributes:
            self.logger.warning("Feature '%s' not found in node attributes.", feature)
            return {}

        # Filter out invalid (e.g., NaN) values and get unique feature values
        unique_feature_values = {
            value for value in node_attributes.values() if value == value
        }

        # Create subgraphs for each unique feature value
        feature_values_subgraphs = {
            feature_value: self.g.subgraph(
                [
                    node
                    for node, data in self.g.nodes(data=True)
                    if data.get(feature) == feature_value
                ]
            )
            for feature_value in unique_feature_values
        }

        return feature_values_subgraphs

    def avg_similarity_4_nodes_by_graph(self, subgraph: nx.Graph) -> dict[int, float]:
        """
        Calculates the average similarity of each node in a subgraph.

        If the subgraph is a clique, all nodes have an average similarity of 1.
        Otherwise, the similarity is calculated based on the specified algorithm.

        Args:
            subgraph (nx.Graph): The input subgraph.

        Returns:
            dict[int, float]: A dictionary mapping each node to its average similarity.

        Raises:
            ValueError: If the specified similarity algorithm is not supported.
        """
        # Early return for cliques
        if self.is_clique(subgraph):
            return {node: 1.0 for node in subgraph.nodes}

        # Map algorithm names to functions
        similarity_algorithms = {
            "jaccard": jaccard_coefficient,
            "adamic_adar": adamic_adar_index,
            "weighted_jaccard": weighted_jaccard_coefficient,
            "weighted_adamic_adar": weighted_adamic_adar_index,
        }

        # Validate algorithm
        similarity_fn = similarity_algorithms.get(self.similarity_algorithm)
        if not similarity_fn:
            raise ValueError(
                f"Unsupported similarity algorithm: {self.similarity_algorithm}"
            )

        # Compute similarities
        sum_similarity = defaultdict(float)
        node_neighbors = defaultdict(float)

        for u, v, similarity in similarity_fn(subgraph):
            if similarity > 0:  # Ignore zero similarities
                sum_similarity[u] += similarity
                sum_similarity[v] += similarity
                node_neighbors[u] += 1
                node_neighbors[v] += 1

        # Calculate average similarities
        return {
            node: sum_similarity[node] / node_neighbors[node] for node in sum_similarity
        }

    def get_similarity_4_feature(self, feature: str) -> pd.DataFrame:
        """
        Computes the average similarity coefficient for each value of the given feature.

        For each distinct value of the feature, the subgraph induced by nodes
        with that value is extracted, and the average similarity of nodes in
        the subgraph is calculated.

        Args:
            feature (str): The feature to analyze.

        Returns:
            pd.DataFrame: A DataFrame with MultiIndex columns representing the feature label and
                            its corresponding similarity values. Returns an empty DataFrame if no
                            valid similarities are found.
        """
        feature_values_subgraphs = self.get_subgraphs(feature)
        subgraph_similarity_data = []

        for feature_value, subgraph in feature_values_subgraphs.items():
            avg_similarity = self.avg_similarity_4_nodes_by_graph(subgraph)
            if not avg_similarity:  # Skip if there are no valid similarities
                continue

            # Collect data for the current feature value
            subgraph_similarity_data.append(
                pd.DataFrame(
                    {
                        feature: feature_value,
                        "value": list(avg_similarity.values()),
                    },
                    index=list(avg_similarity.keys()),
                )
            )

        if not subgraph_similarity_data:
            return pd.DataFrame()

        # Concatenate all the collected data
        feature_similarity_df = pd.concat(subgraph_similarity_data)

        # Assign MultiIndex columns
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
