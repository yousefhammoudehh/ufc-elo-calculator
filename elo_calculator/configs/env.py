from os import getenv

from elo_calculator.utils.converters import convert_to_int

DB_HOST = getenv('DB_HOST', 'ufc-elo-calculator-postgres')
DB_NAME = getenv('DB_NAME', 'postgres')
DB_PASSWORD = getenv('DB_PASSWORD', 'postgres')
DB_PORT = getenv('DB_PORT', '5432')
DB_USERNAME = getenv('DB_USERNAME', 'postgres')
DB_POOL_SIZE = convert_to_int(getenv('DB_POOL_SIZE', '5'), 5)
DB_MAX_OVERFLOW = convert_to_int(getenv('DB_MAX_OVERFLOW', '5'), 5)
DB_POOL_TIMEOUT = convert_to_int(getenv('DB_POOL_TIMEOUT', '10'), 10)
DB_POOL_RECYCLE = convert_to_int(getenv('DB_POOL_RECYCLE', '3600'), 3600)
DB_POOL_PRE_PING = getenv('DB_POOL_PRE_PING', 'true').lower() == 'true'
DB_ECHO = getenv('DB_ECHO', 'false').lower() == 'true'
ENVIRONMENT = getenv('ENVIRONMENT')
LOG_LEVEL = getenv('LOG_LEVEL', 'INFO')

REDIS_HOST = getenv('REDIS_HOST', '')
REDIS_PORT = getenv('REDIS_PORT', '6379')
REDIS_MAX_CONNECTIONS = convert_to_int(getenv('REDIS_MAX_CONNECTIONS', '20'), 20)
REDIS_TTL = convert_to_int(getenv('REDIS_TTL', '21600'), 21600)  # 6 Hours
CACHING_SECRET = getenv('CACHING_SECRET', 'secret-key')
