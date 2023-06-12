#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import uuid

import hail as hl

from hail_scripts.reference_data.combine import join_hts, update_existing_joined_hts
from hail_scripts.reference_data.config import GCS_PREFIXES, AccessControl
from hail_scripts.utils.hail_utils import write_ht

from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.vep import run_vep


COMBINED_REFERENCE_HT_PATH = 'reference_datasets/combined.ht'
DATASETS = [
    'cadd',
    'clinvar',
    'dbnsfp',
    'eigen',
    'exac',
    'gnomad_genomes',
    'gnomad_exomes',
    'mpc',
    'primate_ai',
    'splice_ai',
    'topmed',
]


def run(
    env: Env,
    reference_genome: ReferenceGenome,
    dataset: str | None,
    vep_config_json_path: str | None,
):
    destination_path = os.path.join(
        GCS_PREFIXES[(env, AccessControl.PUBLIC)],
        COMBINED_REFERENCE_HT_PATH,
    ).format(
        genome_version=reference_genome.v02_value,
    )
    if (
        hl.hadoop_exists(os.path.join(destination_path, '_SUCCESS'))
        and dataset is not None
    ):
        ht = update_existing_joined_hts(
            destination_path,
            dataset,
            DATASETS,
            VERSION,
            reference_genome.v02_value,
        )
        unannotated_rows_ht = ht.filter(~hl.is_defined(ht.vep))
        unannotated_rows_ht = run_vep(
            unannotated_rows_ht,
            env,
            reference_genome,
            DatasetType.SNV,
            vep_config_json_path,
        )
        ht = ht.union(unannotated_rows_ht)
    else:
        ht = join_hts(DATASETS, VERSION, reference_genome=reference_genome.v02_value)
        run_vep(
            ht,
            env,
            reference_genome,
            DatasetType.SNV,
            vep_config_json_path,
        )

    ht.describe()
    checkpoint_path = f"{GCS_PREFIXES[('dev', AccessControl.PUBLIC)]}/{uuid.uuid4()}.ht"
    print(f'Checkpointing ht to {checkpoint_path}')
    ht = ht.checkpoint(checkpoint_path, stage_locally=True)
    print(f'Uploading ht to {destination_path}')
    write_ht(ht, destination_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--env',
        type=Env,
        choices=list(Env),
        default=Env.DEV,
    )
    parser.add_argument(
        '--reference-genome',
        type=ReferenceGenome,
        choices=list(ReferenceGenome),
        default=ReferenceGenome.GRCh38,
    )
    parser.add_argument(
        '--dataset',
        choices=DATASETS,
        required=True,
    )
    parser.add_argument(
        '--vep-config-json-path',
        default=None,
    )
    args, _ = parser.parse_known_args()
    run(
        args.env,
        args.reference_genome,
        args.dataset,
        args.vep_config_json_path,
    )
