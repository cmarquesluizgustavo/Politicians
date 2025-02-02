import os
import argparse
import time
from multiprocessing import Pool
from base_logger import NetworkAnalyzerLogger
from task_processor import process_task

MAX_PROCESSES = 8
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
    Runs processes to get statistics for all networks using a process pool.
    """
    os.makedirs(save_path, exist_ok=True)
    os.makedirs(f"{save_path}/networks", exist_ok=True)
    os.makedirs(f"{save_path}/nodes", exist_ok=True)

    logger.info("Creating process pool to get statistics for all networks.")
    
    # Prepare arguments for each task
    task_args = [
        (file, target_features, similarity_algorithms, save_path)
        for file in files
    ]

    with Pool(processes=MAX_PROCESSES) as pool:
        try:
            results = pool.map_async(process_task, task_args)
            results.wait()
        except KeyboardInterrupt:
            logger.warning("Received interrupt, terminating processes...")
            pool.terminate()
            raise
        except Exception as e:
            logger.error("Error in process pool: %s", str(e))
            pool.terminate()
            raise

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
        FILES = [f"../../data/network_builder/{args.file}.pkl"]
        logger.info("Processing arg file %s only", args.file)
    else:
        FILES = [
            f"../../data/network_builder/{file}"
            for file in os.listdir("../../data/network_builder")
            if file.endswith(".pkl") 
        ]
    FILES.sort()
    SAVE_PATH = "data/network_analyzer"
    
    try:
        main(FILES, TARGET_FEATURES, SIMILARITY_ALGORITHMS, SAVE_PATH)
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user")
        exit(1)
    except Exception as e:
        logger.error("Fatal error: %s", str(e))
        exit(1)