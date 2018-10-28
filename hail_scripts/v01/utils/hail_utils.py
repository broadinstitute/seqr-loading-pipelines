import hail
import logging
import time

logger = logging.getLogger()


def create_hail_context(verbose=True):
    if verbose:
        logger.info("\n==> create HailContext")

    return hail.HailContext(log="./hail_{}.log".format(time.strftime("%y%m%d_%H%M%S")))


def stop_hail_context(hc):
    try:
        if hc is not None:
            hc.stop()
    except Exception as e:
        logger.info("Error while stopping hail context %s: %s", hc, e)