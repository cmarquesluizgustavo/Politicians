import os
import time
import threading
from base_logger import NetworkAnalyzerLogger
from consolidate_results import consolidate_files
from run_statistics import get_statistics_4_network

MAX_THREADS = 8

logger = NetworkAnalyzerLogger(
    name="AllNetworks",
    client_class="ThreadRunner",
    log_level=20,
    log_file="logs/network_analyzer/thread_runner/thread_runner.log",
    terminal=True,
)


def main(
    files: list,
    target_features: list,
    similarity_algorithms: list,
    save_path: str,
):
    """
    Runs threads to get statistics for all networks, limiting the number of concurrent threads.
    """

    os.makedirs(save_path, exist_ok=True)
    os.makedirs(f"{save_path}/networks", exist_ok=True)
    os.makedirs(f"{save_path}/nodes", exist_ok=True)

    logger.info("Creating threads to get statistics for all networks.")
    threads = []
    semaphore = threading.Semaphore(MAX_THREADS)
    for file in files:
        semaphore.acquire()
        t = threading.Thread(
            target=get_statistics_4_network,
            args=(
                file,
                target_features.copy(),
                similarity_algorithms.copy(),
                save_path,
                semaphore,
            ),
        )
        threads.append(t)
        time.sleep(1.5)  # Avoids two threads to have the same name and log file
        t.start()
        logger.info("Thread for file %s started.", file)

    logger.info("Waiting for all threads to finish.")
    for t in threads:
        t.join()

    logger.info("All threads finished.")
    consolidate_files(save_path, similarity_algorithms, target_features)
    logger.info("Files consolidated.")


if __name__ == "__main__":
    TARGET_FEATURES = [
        "siglaPartido",
        "siglaUf",
        "education",
        "gender",
        "region",
        "occupation",
        "ethnicity",
        "age_group",
    ]
    SIMILARITY_ALGORITHMS = ["adamic_adar", "jaccard"]
    FILES = [
        f"data/network_builder/{file}"
        for file in os.listdir("data/network_builder")
        if file.endswith(".pkl")
    ]
    FILES.sort()
    SAVE_PATH = "data/network_analyzer"
    main(FILES, TARGET_FEATURES, SIMILARITY_ALGORITHMS, SAVE_PATH)
