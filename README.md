# Django Custom Logger

A custom logging package for Django applications that provides enhanced logging capabilities, including request ID tracking and size-based log rotation.

## Installation

You can install the package using pip:

```
pip install django-custom-logger
```

## Usage

1. Add `django_custom_logger` to your `INSTALLED_APPS` in your Django settings:

```python
INSTALLED_APPS = [
    ...
    'django_custom_logger',
    ...
]
```

2. Add the middleware to your `MIDDLEWARE` setting:

```python
MIDDLEWARE = [
    ...
    'django_custom_logger.RequestResponseLoggingMiddleware',
    ...
]
```

3. (Optional) Configure custom logger settings in your Django `settings.py`:

```python
# Custom logger settings (optional)
CUSTOM_LOGGER_DIR = os.path.join(BASE_DIR, 'logs')  # Default is BASE_DIR/logs
CUSTOM_LOGGER_FILE = 'my_custom_log.log'  # Default is 'django_custom_logger.log'
CUSTOM_LOGGER_MAX_BYTES = 10 * 1024 * 1024  # Default is 5MB
CUSTOM_LOGGER_KEEP_DAYS = 14  # Default is 30 days
CUSTOM_LOGGER_LEVEL = 'INFO'  # Default is 'DEBUG'
```

4. Use the logger in your code:

```python
import logging

logger = logging.getLogger(__name__)
logger.info('Your log message here')
```

## Features

- Request ID tracking
- Size and time-based log rotation
- Automatic cleanup of old log files
- Request and response logging middleware
- Automatic configuration on app startup

## License

This project is licensed under the MIT License - see the LICENSE file for details.