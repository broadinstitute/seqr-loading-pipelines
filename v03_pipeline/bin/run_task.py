#!/usr/bin/env python3
import sys

import luigi

import v03_pipeline.tasks # noqa: F401

if __name__ == '__main__':
    luigi.run() or sys.exit(1)
