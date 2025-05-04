import functools
import time

from v03_pipeline.lib.logger import get_logger

logger = get_logger(__name__)


def retry(tries=3, delay=1, backoff=2):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal tries, delay
            while tries > 1:
                try:
                    return func(*args, **kwargs)
                except Exception as e:  # noqa: BLE001
                    msg = (
                        f'{func.__name__} failed with {e}, retrying in {delay} seconds.'
                    )
                    logger.info(msg)
                    time.sleep(delay)
                    tries -= 1
                    delay *= backoff
            # Final attempt
            return func(*args, **kwargs)

        return wrapper

    return decorator
