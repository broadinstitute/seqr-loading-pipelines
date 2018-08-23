import hail
import logging
import time

logger = logging.getLogger()


def create_hail_context(verbose=True):
    if verbose:
        logger.info("\n==> create HailContext")

    return hail.HailContext(log="/hail_{}.log".format(time.strftime("%y%m%d_%H%M%S")))

