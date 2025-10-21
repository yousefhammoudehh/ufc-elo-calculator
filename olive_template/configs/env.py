from os import getenv

DB_HOST = getenv('DB_HOST', 'olive-template-postgres')
DB_NAME = getenv('DB_NAME', 'postgres')
DB_PASSWORD = getenv('DB_PASSWORD', 'postgres')
DB_PORT = getenv('DB_PORT', '5432')
DB_USER = getenv('DB_USER', 'postgres')
ENVIRONMENT = getenv('ENVIRONMENT')
IDENTITY_PROVIDER_URL = getenv('IDENTITY_PROVIDER_URL', '')
LOG_LEVEL = getenv('LOG_LEVEL', 'INFO')
