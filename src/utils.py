import logging

class BaseLogger(logging.Logger):
    def __init__(self, log_level: str = None, log_file: str = None, **kwargs):
        super().__init__(kwargs.get('logger', 'BaseLogger'))
        self.handle_logging(log_level, log_file, kwargs.get('terminal', False))

    def handle_logging(self, log_level, log_file, terminal) -> None:
        self.log_level = log_level
        self.log_file = log_file
        self.setLevel(self.log_level)
        self.addHandler(logging.FileHandler(self.log_file))
        if terminal:
            self.addHandler(logging.StreamHandler())
        self.info(f"Initializing logger - Log level: {self.log_level} - Log file: {self.log_file}")