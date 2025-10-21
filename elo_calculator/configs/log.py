import os
import sys
from typing import Any

from loguru import logger

LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_MESSAGE_FORMAT = ('%(asctime)s,%(msecs)03d [%(process)s] [%(thread)d] [%(name)s] '
                      '[%(filename)s:%(lineno)d] %(levelname)s %(message)s')

LOGURU_MESSAGE_FORMAT = '{time:YYYY-MM-DD HH:mm:ss,SSS} [{process}] [{thread}] '\
                        '[{extra[clickable_path]}] {level} {message} {exception}\n'

LOGURU_MESSAGE_FORMAT_DEV = '{time:YYYY-MM-DD HH:mm:ss,SSS} [{process}] [{thread}] '\
                            '<level>[{extra[clickable_path]}] '\
                            '{level} {message}{exception}</level>\n'


def env_is_dev() -> bool:
    return os.getenv('ENVIRONMENT') == 'dev'


def log_level() -> str:
    return os.getenv('LOG_LEVEL', 'INFO')


def log_formatter(record: dict[str, Any]) -> str:
    clickable_path = record['name'].replace('.', '/') + '.py:' + str(record['line'])
    record['extra']['clickable_path'] = clickable_path
    return LOGURU_MESSAGE_FORMAT_DEV if env_is_dev() else LOGURU_MESSAGE_FORMAT


def create_handlers(level: str = log_level(),
                    if_dev: bool = env_is_dev()
                    ) -> dict[str, Any]:
    return {
        'handlers': [
            {'sink': sys.stdout, 'format': log_formatter,
             'level': level, 'colorize': if_dev,
             'backtrace': True, 'diagnose': if_dev}
        ]}


def get_logger() -> Any:
    logger.configure(**create_handlers())
    return logger
