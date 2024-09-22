import os
from django.conf import settings
from .handlers import create_timed_rotating_log_handler

def get_logging_config():
    LOG_DIR = getattr(settings, 'CUSTOM_LOGGER_DIR', os.path.join(settings.BASE_DIR, 'logs'))
    LOG_FILE = getattr(settings, 'CUSTOM_LOGGER_FILE', 'api_logs.log')
    LOG_PATH = os.path.join(LOG_DIR, LOG_FILE)
    LOG_MAX_BYTES = getattr(settings, 'CUSTOM_LOGGER_MAX_BYTES', 5 * 1024 * 1024)  # Default 5MB
    LOG_KEEP_DAYS = getattr(settings, 'CUSTOM_LOGGER_KEEP_DAYS', 30)
    LOG_LEVEL = getattr(settings, 'CUSTOM_LOGGER_LEVEL', 'DEBUG')

    LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'verbose': {
                'format': '{asctime} [{request_id}] {levelname} {module} {process} {thread} {funcName} {name}:{lineno} {message}',
                'style': '{',
            },
        },
        'filters': {
            'request_id': {
                '()': 'drf_advanced_logger.handlers.RequestIdFilter',
            },
        },
        'handlers': {
            'custom_file': {
                '()': create_timed_rotating_log_handler,
                'filename': LOG_PATH,
                'max_bytes': LOG_MAX_BYTES,
                'keep_days': LOG_KEEP_DAYS,
                'formatter': 'verbose',
                'filters': ['request_id'],
            },
            'console': {
                'level': LOG_LEVEL,
                'class': 'logging.StreamHandler',
                'formatter': 'verbose',
                'filters': ['request_id'],
            },
        },
        'loggers': {
            '': {
                'handlers': ['custom_file', 'console'],
                'level': LOG_LEVEL,
                'propagate': True,
            },
        },
    }

    return LOGGING