from django.apps import AppConfig
from django.conf import settings
from .config import get_logging_config
import logging.config

class DjangoCustomLoggerConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'drf_advanced_logger'

    def ready(self):
        # Configure logging when the app is ready
        logging_config = get_logging_config()
        logging.config.dictConfig(logging_config)