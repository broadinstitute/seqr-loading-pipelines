#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

import hail as hl

from hail_scripts.reference_data.combine import join_hts, update_existing_joined_hts

from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.model import Env, ReferenceDatasetCollection, ReferenceGenome
from v03_pipeline.lib.paths import valid_reference_dataset_collection_path


def run(reference_genome: ReferenceGenome, dataset: str | None):
    destination_path = valid_reference_dataset_collection_path(
        reference_genome,
        ReferenceDatasetCollection.COMBINED,
    )
    hl.init(tmp_dir=Env.HAIL_TMPDIR)
    hl._set_flags(  # noqa: SLF001
        no_whole_stage_codegen='1',
    )  # hail 0.2.78 hits an error on the join, this flag gets around it
    if (
        hl.hadoop_exists(os.path.join(destination_path, '_SUCCESS'))
        and dataset is not None
    ):
        ht = update_existing_joined_hts(
            destination_path,
            dataset,
            ReferenceDatasetCollection.COMBINED,
            reference_genome,
        )
    else:
        ht = join_hts(
            ReferenceDatasetCollection.COMBINED,
            reference_genome,
        )
    ht.describe()
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
    parser.add_argument(
        '--dataset',
        choices=ReferenceDatasetCollection.COMBINED.datasets,
        default=None,
        help='When passed, update the single dataset, otherwise update all datasets.',
    )
    args, _ = parser.parse_known_args()
    run(
        args.reference_genome,
        args.dataset,
    )
