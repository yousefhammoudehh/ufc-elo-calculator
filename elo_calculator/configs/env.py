from os import getenv

from elo_calculator.utils.converters import convert_to_float, convert_to_int

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

# Scraper configuration
SCRAPER_MIN_INTERVAL_SECONDS: float = convert_to_float(getenv('SCRAPER_MIN_INTERVAL_SECONDS', '2.0'), 2.0)
SCRAPER_CB_THRESHOLD: int = convert_to_int(getenv('SCRAPER_CB_THRESHOLD', '5'), 5)
SCRAPER_CB_COOLDOWN_SECONDS: float = convert_to_float(getenv('SCRAPER_CB_COOLDOWN_SECONDS', '1800'), 1800.0)
SCRAPER_BACKOFF_START_SECONDS: float = convert_to_float(getenv('SCRAPER_BACKOFF_START_SECONDS', '60'), 60.0)

_SCRAPER_BACKOFF_JITTER_RAW = getenv('SCRAPER_BACKOFF_JITTER', '0.8,1.2')
# Robust parsing: allow one or two values; fallback to defaults on errors
try:
    parts = [p.strip() for p in _SCRAPER_BACKOFF_JITTER_RAW.split(',') if p.strip()]
    if not parts:
        _jit_lo, _jit_hi = 0.8, 1.2
    elif len(parts) == 1:
        v = convert_to_float(parts[0], 0.8)
        _jit_lo, _jit_hi = v, v
    else:
        v1 = convert_to_float(parts[0], 0.8)
        v2 = convert_to_float(parts[1], 1.2)
        _jit_lo, _jit_hi = v1, max(v1, v2)
    SCRAPER_BACKOFF_JITTER: tuple[float, float] = (_jit_lo, _jit_hi)
except Exception:
    SCRAPER_BACKOFF_JITTER = (0.8, 1.2)

_DEFAULT_UAS = '|'.join(
    [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0',
    ]
)
SCRAPER_USER_AGENTS: list[str] = [ua for ua in (getenv('SCRAPER_USER_AGENTS', _DEFAULT_UAS).split('|')) if ua.strip()]

_DEFAULT_LANGS = 'en-US,en;q=0.9,en-GB,en;q=0.8,en,en-US;q=0.8'
SCRAPER_ACCEPT_LANGUAGES: list[str] = [
    lang.strip() for lang in getenv('SCRAPER_ACCEPT_LANGUAGES', _DEFAULT_LANGS).split(',') if lang.strip()
]
