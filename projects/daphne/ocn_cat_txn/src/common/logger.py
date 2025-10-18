import logging

class Logger:
    def __init__(self) -> None:
        self.logger = logging.getLogger("ETL")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            Logger.__add_console_handler()

    @staticmethod
    def __add_console_handler() -> None:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        console_handler.setFormatter(formatter)
        logging.getLogger("ETL").addHandler(console_handler)

    def info(self, message: str) -> None:
        self.logger.info(message)

    def error(self, message: str) -> None:
        self.logger.error(message)
    
    def warning(self, message: str) -> None:
        self.logger.warning(message)

    def debug(self, message: str) -> None:
        self.logger.debug(message)

logger = Logger()
