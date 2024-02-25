import pickle
import pandas as pd
import networkx as nx
from .basic_statistics import BasicStatistics
from .similarity_statistics import SimilarityStatistics


class GetResults:
    """
    This class calls the basicStatistics and featuresGains classes to get the results.
    The specific for the whole network, or to a specific subgraph or a node
    """

    def __init__(self, g: nx.Graph, target_features: list, similarity_algorithm: str):
        self.g = g
        self.target_features = target_features

        self.basic_statistics = BasicStatistics(g)
        self.features_gains = SimilarityStatistics(
            g, target_features, similarity_algorithm
        )
        gains_by_feature = {}
        for feature in self.target_features:
            gains_by_feature[feature] = self.get_gains_by_feature(feature)

        self.gains_by_node = pd.concat(
            [self.get_gains_by_node(node) for node in self.g.nodes()]
        )

    def get_gains_by_feature(self, feature: str) -> pd.DataFrame:
        """'
        Calculate the gain of an specific feature
        """
        feature_gain = {}
        for feature_label in self.features_gains.similarity[feature]["label"].unique():
            feature_gain[feature_label] = self.features_gains.similarity[
                self.features_gains.similarity[feature]["label"] == feature_label
            ][feature]["gain"].mean()
        feature_gain["global"] = self.features_gains.similarity[feature]["gain"].mean()
        feature_gain = pd.DataFrame(feature_gain, index=[feature])
        return feature_gain

    def get_gains_by_node(self, node: str) -> pd.DataFrame:
        """'
        Calculate the gains of an specific node for each feature
        """
        # Returns empty dataframe if node not in the list of gains
        if node not in self.features_gains.similarity.index:
            return pd.DataFrame()
        node_gain = self.features_gains.similarity[
            self.features_gains.similarity.index == node
        ]
        # Get only the gains of the target features
        node_gain = node_gain[[(feature, "gain") for feature in self.target_features]]
        node_gain.columns = node_gain.columns.droplevel(1)

        return pd.DataFrame(node_gain, index=[node])


if __name__ == "__main__":
    g_ = pickle.load(open("data/networks/2023.pkl", "rb"))
    gr = GetResults(
        g_,
        target_features=[
            "siglaPartido",
            "siglaUf",
            "education",
            "gender",
            "region",
            "occupation",
            "ethnicity",
        ],
        similarity_algorithm="jaccard",
    )
