#!/usr/bin/env python3
import sys

import luigi

from v03_pipeline.tasks.variant_annotations_table import ( # noqa: F401
    UpdateVariantAnnotationsTableWithNewSamples,
    UpdateVariantAnnotationsTableWithReferenceData,
)

if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
