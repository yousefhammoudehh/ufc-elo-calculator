from os import getenv

ENVIRONMENT = getenv('ENVIRONMENT', 'dev')
LOG_LEVEL = getenv('LOG_LEVEL', 'INFO')
API_PREFIX = getenv('API_PREFIX', '')
