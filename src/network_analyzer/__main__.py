import os
import argparse
import time
import multiprocessing
from base_logger import NetworkAnalyzerLogger
from consolidate_results import consolidate_files
from run_statistics import get_statistics_4_network

MAX_PROCESSES = 8
logger = NetworkAnalyzerLogger(
    name="AllNetworks",
    client_class="ThreadRunner",
    log_level=20,
    log_file="logs/network_analyzer/thread_runner/thread_runner.log",
    terminal=True,
)


def process_task(file, target_features, similarity_algorithms, save_path, semaphore):
    """
    Task function to be executed in a separate process.
    """
    with semaphore:  # Use a context manager to release the semaphore automatically
        logger.info("Process for file %s started.", file)
        try:
            get_statistics_4_network(
                file,
                target_features.copy(),
                similarity_algorithms.copy(),
                save_path,
                logger,
            )
            logger.info("Process for file %s completed.", file)
        except Exception as e:
            logger.error("Error in process for file %s: %s", file, str(e))


def main(
    files: list,
    target_features: list,
    similarity_algorithms: list,
    save_path: str,
):
    """
    Runs processes to get statistics for all networks, limiting the number of concurrent processes.
    """

    os.makedirs(save_path, exist_ok=True)
    os.makedirs(f"{save_path}/networks", exist_ok=True)
    os.makedirs(f"{save_path}/nodes", exist_ok=True)

    logger.info("Creating processes to get statistics for all networks.")
    processes = []
    semaphore = multiprocessing.Semaphore(MAX_PROCESSES)

    for file in files:
        p = multiprocessing.Process(
            target=process_task,
            args=(
                file,
                target_features.copy(),
                similarity_algorithms.copy(),
                save_path,
                semaphore,
            ),
        )
        processes.append(p)
        time.sleep(1.5)  # Avoids two processes to have the same name and log file
        p.start()

    logger.info("Waiting for all processes to finish.")
    for p in processes:
        p.join()

    logger.info("All processes finished.")
    consolidate_files(save_path, similarity_algorithms, target_features)
    logger.info("Files consolidated.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some files.")
    parser.add_argument("--file", type=str, help="Specific file to process")

    args = parser.parse_args()

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
    SIMILARITY_ALGORITHMS = [
        "weighted_adamic_adar",
        "weighted_jaccard",
        "jaccard",
        "adamic_adar",
    ]
    if args.file:
        FILES = [f"data/network_builder/{args.file}.pkl"]
        logger.info("Processing arg file %s only", args.file)

    else:
        # networks_to_run = [52, 53, 54, 55, 56, 57]
        networks_not_to_run = [1999,
                2001,
                2000,
                2003,
                2002,
                2008,
                2004,
                2005,
                2007,
                2009,
                2006,
                2012,
                2010,
                2013,
                2011,
                2015,
                2014,
                2015
                ]
        FILES = [
            f"data/network_builder/{file}"
            for file in os.listdir("data/network_builder")
            if file.endswith(".pkl")  # and int(file.split(".")[0]) in networks_to_run
            # and int(file.split(".")[0]) not in networks_not_to_run
        ]
    FILES.sort()
    SAVE_PATH = "data/network_analyzer"
    main(FILES, TARGET_FEATURES, SIMILARITY_ALGORITHMS, SAVE_PATH)

# debug
# files = ["data/network_builder/2024.pkl"]
# target_features = [
#     "siglaPartido",
#     "siglaUf",
#     "education",
#     "gender",
#     "region",
#     "occupation",
#     "ethnicity",
#     "age_group",
# ]
# similarity_algorithms = [
#     "weighted_jaccard",
# ]
# similarity_algorithm = similarity_algorithms[0]
# save_path = "data/network_analyzer"
# file = files[0]
