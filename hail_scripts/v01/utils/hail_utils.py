import hail
import time


def create_hail_context():
    return hail.HailContext(log="/hail_{}.log".format(time.strftime("%y%m%d_%H%M%S")))

