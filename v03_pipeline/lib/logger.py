import logging.config

LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True,
    'propagate': True,
    'formatters': {
        'default': {
            'format': '%(asctime)s - %(module)s - %(levelname)s - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
    },
    'handlers': {
        'default': {
            'formatter': 'default',
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        '': {
            'level': 'INFO',
            'handlers': ['default'],
            'propagate': True,
        },
    },
}

_CONFIGURED = False

def get_logger(name: str):
    global _CONFIGURED # noqa: PLW0603
    if not _CONFIGURED:
        logging.config.dictConfig(LOG_CONFIG)
        _CONFIGURED = True
    return logging.getLogger(name)