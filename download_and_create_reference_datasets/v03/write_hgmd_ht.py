#!/usr/bin/env python3
import argparse

from hail_scripts.reference_data.combine import join_hts

from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import valid_reference_dataset_collection_path


def run(reference_genome: ReferenceGenome):
    ht = join_hts(
        reference_genome,
        DatasetType.SNV_INDEL,
        ReferenceDatasetCollection.HGMD,
    )
    destination_path = valid_reference_dataset_collection_path(
        reference_genome,
        DatasetType.SNV_INDEL,
        ReferenceDatasetCollection.HGMD,
    )
    print(f'Uploading ht to {destination_path}')
    write(ht, destination_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--reference-genome',
        type=ReferenceGenome,
        choices=list(ReferenceGenome),
        default=ReferenceGenome.GRCh38,
    )
    args, _ = parser.parse_known_args()
    run(args.reference_genome)
