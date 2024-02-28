import os
import time
import pickle
import threading
import pandas as pd
from base_logger import NetworkAnalyzerLogger
from basic_statistics import BasicStatistics
from similarity_statistics import SimilarityAndGainsStatistics

MAX_THREADS = 8


def get_statistics_all_networks(
    file: str,
    target_features: list,
    similarity_algorithm: str,
    save_path: str,
    semaphore: threading.Semaphore,
):
    """
    Get and save all statistics for the network
    """
    g = pickle.load(open(file, "rb"))
    bs = BasicStatistics(g)

    sgs = SimilarityAndGainsStatistics(
        g,
        target_features,
        similarity_algorithm,
    )
    node_df = pd.concat(
        [sgs.gains_by_node, bs.nodes_to_dataframe()], join="outer", axis=1
    ).fillna(0)
    node_df.to_csv(
        f"{save_path}/features/{similarity_algorithm}/nodes/{g.name}_nodes.csv"
    )

    bs.network_to_dataframe().to_csv(
        f"{save_path}/networks/{g.name}_network.csv", index=False
    )

    for feature, feature_data in sgs.gains_by_feature.items():
        feature_data.to_csv(
            f"{save_path}/features/{similarity_algorithm}/network/{g.name}_{feature}.csv",
            index=False,
        )

    semaphore.release()


def consolidate_files_per_feature(
    path: str, similarity_algorithm: str, target_features: list
):
    network_features_files = os.listdir(
        f"{path}/features/{similarity_algorithm}/network"
    )

    node_features_files = os.listdir(f"{path}/features/{similarity_algorithm}/nodes")

    nodes_df = pd.concat(
        [
            pd.read_csv(f"{path}/features/{similarity_algorithm}/nodes/{node_file}")
            for node_file in node_features_files
        ],
    )
    nodes_df.to_csv(f"{path}/features/{similarity_algorithm}/nodes.csv")

    for feature in target_features:
        feature_periods = pd.concat(
            [
                pd.read_csv(
                    f"{path}/features/{similarity_algorithm}/network/{network_feature_file}"
                )
                for network_feature_file in network_features_files
                if feature in network_feature_file
            ],
        )
        feature_periods.to_csv(f"{path}/features/{similarity_algorithm}/{feature}")


def consolidate_files(path: str, similarity_algorithms: list, target_features: list):
    """
    This function gathers all the csv files generated by the runs
    and consolidates them into a few csv files that contain the data.
    """

    networks_files = os.listdir(f"{path}/networks")

    networks_df = pd.concat(
        [
            pd.read_csv(f"{path}/networks/{network_file}")
            for network_file in networks_files
        ],
    )
    networks_df.to_csv(f"{path}/networks/networks.csv")

    for similarity_algorithm in similarity_algorithms:
        consolidate_files_per_feature(path, similarity_algorithm, target_features)


def main(
    files: list,
    target_features: list,
    similarity_algorithms: list,
    save_path: str,
):
    """
    Runs threads to get statistics for all networks, limiting the number of concurrent threads.
    """
    logger = NetworkAnalyzerLogger(
        name="AllNetworks",
        client_class="ThreadRunner",
        log_level=20,
        log_file="logs/network_analyzer/thread_runner/thread_runner.log",
        terminal=True,
    )
    os.makedirs(save_path, exist_ok=True)
    os.makedirs(f"{save_path}/nodes", exist_ok=True)
    os.makedirs(f"{save_path}/networks", exist_ok=True)
    os.makedirs(f"{save_path}/features", exist_ok=True)

    logger.info("Creating threads to get statistics for all networks.")
    threads = []
    semaphore = threading.Semaphore(MAX_THREADS)
    for similarity_algorithm in similarity_algorithms:
        for file in files:
            semaphore.acquire()
            t = threading.Thread(
                target=get_statistics_all_networks,
                args=(
                    file,
                    target_features,
                    similarity_algorithm,
                    save_path,
                    semaphore,
                ),
            )
            threads.append(t)
            time.sleep(1.5)  # Avoids two threads to have the same name and log file
            t.start()

    logger.info("Waiting for all threads to finish.")
    for t in threads:
        t.join()

    logger.info("All threads finished.")
    consolidate_files(save_path, similarity_algorithms, target_features)
    logger.info("Files consolidated.")


if __name__ == "__main__":
    target_features = [
        "siglaPartido",
        "siglaUf",
        "education",
        "gender",
        "region",
        "occupation",
        "ethnicity",
    ]
    similarity_algorithms = ["adamic_adar", "jaccard"]
    files = [
        f"data/network_builder/{file}"
        for file in os.listdir("data/network_builder")
        if file.endswith(".pkl")
    ]
    save_path = "data/network_analyzer"
    main(files, target_features, similarity_algorithms, save_path)
