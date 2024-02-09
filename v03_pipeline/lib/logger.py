import json
import logging.config

LOG_CONFIG_FILE = 'v03_pipeline/var/log_conf.json'
_CONFIGURED = False

def get_logger(name: str):
    global _CONFIGURED
    if not _CONFIGURED:
        with open(LOG_CONFIG_FILE) as f:
            logging.config.dictConfig(json.load(f))
        _CONFIGURED = True
    return logging.getLogger(name)
