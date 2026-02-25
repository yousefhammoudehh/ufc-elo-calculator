import logging

from elo_calculator.configs.env import LOG_LEVEL


def get_logger(name: str = 'elo_calculator') -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
        logger.addHandler(handler)
    logger.setLevel(LOG_LEVEL)
    return logger
