import os
import pickle
import math
from base_logger import NetworkAnalyzerLogger
from basic_statistics import BasicStatistics
from similarity_statistics import SimilarityAndGainsStatistics


def get_statistics_4_network(
    file: str,
    target_features: list,
    similarity_algorithms: list,
    save_path: str,
    logger: NetworkAnalyzerLogger,
):
    """
    Get and save all statistics for the network
    """
    try:
        g = pickle.load(open(file, "rb"))
    except Exception as e:
        logger.error("Error loading file %s: %s", file, e)
        return
    
    try:
        bs = BasicStatistics(g)
    except Exception as e:
        logger.error("Error getting basic statistics for file %s: %s", file, e)
    
    try:
        bs.network_to_dataframe().to_csv(
            f"{save_path}/networks/{g.name}_network.csv", index=False
        )
        bs.nodes_to_dataframe().to_csv(
            f"{save_path}/nodes/{g.name}_nodes.csv", index=True
        )
    except Exception as e:
        logger.error("Error saving basic statistics for file %s: %s", file, e)

    try:
        for similarity_algorithm in similarity_algorithms:
            try:
                get_statistics_4_similarity_algorithm(
                    g, target_features, similarity_algorithm, save_path
                )
            except Exception as e:
                logger.error("Error getting statistic %s for file %s: %s", similarity_algorithm, file, e)

    except Exception as e:
        logger.error("Error getting statistics for file %s: %s", file, e)


def get_statistics_4_similarity_algorithm(
    g,
    target_features,
    similarity_algorithm,
    save_path,
):
    """
    Get and save all statistics for the network using a specific similarity algorithm
    """
    os.makedirs(f"{save_path}/features/{similarity_algorithm}/network", exist_ok=True)
    os.makedirs(f"{save_path}/features/{similarity_algorithm}/nodes", exist_ok=True)

    sgs = SimilarityAndGainsStatistics(
        g,
        target_features,
        similarity_algorithm,
    )
    sgs.gains_by_node.to_csv(
        f"{save_path}/features/{similarity_algorithm}/nodes/{g.name}_nodes.csv"
    )

    for feature, feature_data in sgs.gains_by_feature.items():
        feature_data.to_csv(
            f"{save_path}/features/{similarity_algorithm}/network/{g.name}_{feature}.csv",
            index=False,
        )
