import os
import logging
from datetime import datetime


class NetworkAnalyzerLogger(logging.Logger):
    """
    Base class for loggers. Inherits from the logging.Logger class.
    Deals with logging configurations.
    """

    def __init__(self, name, client_class, log_level: int, log_file: str, **kwargs):
        """
        Initialize the BaseLogger.
        """
        if name == "":
            raise ValueError("Name cannot be empty.")
        super().__init__(kwargs.get("logger", "NetworkAnalyzerLogger"))
        self.handle_logging(
            name, client_class, log_level, log_file, kwargs.get("terminal", False)
        )

    def info(self, msg, *args, **kwargs):
        """
        Log an info message. Custom to add a timestamp to the message.
        """
        super().info(
            f"{datetime.now().strftime('%Y-%m-%d@%H:%M:%S')} - {msg}", *args, **kwargs
        )

    def handle_logging(self, name, client_class, log_level, log_file, terminal) -> None:
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
            "Initializing %s logger for network %s\nLog level: %s - Log file: %s\n",
            client_class,
            name,
            logging.getLevelName(self.log_level),
            self.log_file,
        )
