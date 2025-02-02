from base_logger import NetworkAnalyzerLogger
from consolidate_results import consolidate_files
from run_statistics import get_statistics_4_network

logger = NetworkAnalyzerLogger(
    name="TaskProcessor",
    client_class="ThreadRunner",
    log_level=20,
    log_file="logs/network_analyzer/thread_runner/task_processor.log",
    terminal=True,
)

def process_task(args):
    """
    Task function to be executed in the process pool.
    """
    file, target_features, similarity_algorithms, save_path = args
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
        return True
    except Exception as e:
        logger.error("Error in process for file %s: %s", file, str(e))
        return False