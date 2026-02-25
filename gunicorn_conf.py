from os import getenv

ENVIRONMENT = getenv('ENVIRONMENT', 'development')
LOG_LEVEL = getenv('LOG_LEVEL', 'INFO')
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_MESSAGE_FORMAT = '%(asctime)s %(levelname)s %(name)s %(message)s'

is_dev = ENVIRONMENT in {'development', 'dev'}

bind = '0.0.0.0:80'
reload = is_dev
worker_class = 'uvicorn.workers.UvicornWorker'
workers = 1 if is_dev else 2
max_requests = 2048
max_requests_jitter = 256

accesslog = '-' if worker_class else None

logconfig_dict = {
    'version': 1,
    'formatters': {'generic': {'format': LOG_MESSAGE_FORMAT, 'datefmt': LOG_DATE_FORMAT, 'class': 'logging.Formatter'}},
    'handlers': {'console': {'class': 'logging.StreamHandler', 'formatter': 'generic', 'stream': 'ext://sys.stdout'}},
    'loggers': {
        'root': {'level': LOG_LEVEL, 'handlers': ['console']},
        'gunicorn.error': {'level': LOG_LEVEL, 'handlers': ['console'], 'propagate': False},
    },
}
