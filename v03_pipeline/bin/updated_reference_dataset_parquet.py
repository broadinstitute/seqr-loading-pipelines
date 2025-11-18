#!/usr/bin/env python3
import argparse

import luigi

from v03_pipeline.lib.core import ReferenceGenome
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset_parquet import (
    UpdatedReferenceDatasetParquetTask,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--reference-genome', required=True, type=ReferenceGenome)
    parser.add_argument(
        '--reference-dataset',
        required=True,
        type=ReferenceDataset,
    )
    args = parser.parse_args()
    luigi.build(
        UpdatedReferenceDatasetParquetTask(
            reference_genome=args.reference_genome,
            reference_dataset=args.reference_dataset,
        ),
    )


if __name__ == '__main__':
    main()
