import os
import logging
from datetime import datetime


class NetworkAnalyzerLogger(logging.Logger):
    """
    Base class for loggers. Inherits from the logging.Logger class.
    Deals with logging configurations.
    """

    def __init__(self, name, log_level: int, log_file: str, **kwargs):
        """
        Initialize the BaseLogger.
        """
        if name == "":
            raise ValueError("Name cannot be empty.")
        super().__init__(kwargs.get("logger", "NetworkAnalyzerLogger"))
        self.handle_logging(name, log_level, log_file, kwargs.get("terminal", False))

    def handle_logging(self, name, log_level, log_file, terminal) -> None:
        """
        Set up logging configurations.
        """
        self.log_level = log_level
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        parts = log_file.split("/")
        parts[-1] = f"%s-%s" % (datetime.now().strftime("%Y-%m-%d@%H:%M:%S"), parts[-1])
        self.log_file = "/".join(parts)
        self.setLevel(self.log_level)
        self.addHandler(logging.FileHandler(self.log_file))
        if terminal:
            self.addHandler(logging.StreamHandler())
        self.info(
            "Initializing logger %s - Log level: %s - Log file: %s",
            name,
            logging.getLevelName(self.log_level),
            self.log_file,
        )
