import logging

class Logger:
    def __init__(self) -> None:
        self.logger = logging.getLogger("ETL")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            self.__add_console_handler()

    @staticmethod
    def __add_console_handler() -> None:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
        )
        console_handler.setFormatter(formatter)
        logging.getLogger("ETL").addHandler(console_handler)

    def info(self, msg: str, *args, **kwargs) -> None:
        self.logger.info(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        self.logger.error(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        self.logger.warning(msg, *args, **kwargs)

    def debug(self, msg: str, *args, **kwargs) -> None:
        self.logger.debug(msg, *args, **kwargs)


# Global kullanılacak logger instance
logger = Logger()
