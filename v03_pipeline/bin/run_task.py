#!/usr/bin/env python3
import sys

import luigi

from v03_pipeline.tasks.variant_annotations_table import (  # noqa: F401
    UpdateVariantAnnotationsTableWithNewProject,
    UpdateVariantAnnotationsTableWithReferenceData,
)

if __name__ == '__main__':
    luigi.run() or sys.exit(1)
