#!/usr/bin/env python3
import sys

import luigi

from v03_pipeline.lib.tasks import *

if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)