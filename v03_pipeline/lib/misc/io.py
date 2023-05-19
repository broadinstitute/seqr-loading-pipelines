from __future__ import annotations
import os
import tempfile
import uuid

import hail as hl

from v03_pipeline.lib.definitions import ReferenceGenome
from v03_pipeline.lib.definitions import DataRoot, Env


def import_vcf(vcf_path: str, reference_genome: ReferenceGenome) -> hl.MatrixTable:
    # Import the VCFs from inputs. Set min partitions so that local pipeline execution takes advantage of all CPUs.
    recode = {}
    if reference_genome == ReferenceGenome.GRCh38:
        recode = {f'{i}': f'chr{i}' for i in ([*list(range(1, 23)), 'X', 'Y'])}
    else:
        recode = {f'chr{i}': f'{i}' for i in ([*list(range(1, 23)), 'X', 'Y'])}
    return hl.import_vcf(
        vcf_path,
        reference_genome=reference_genome.value,
        skip_invalid_loci=True,
        contig_recoding=recode,
        force_bgz=True,
        min_partitions=500,
    )

def import_pedigree(pedigree_path: str) -> hl.Table:
    ht = hl.import_table(pedigree_path)
    ht = ht.select(
        family_id=ht.Family_ID,
        s=ht.Individual_ID,
    )
    return ht.key_by(ht.family_id)

def write_ht(env: Env, ht: hl.Table, destination_path: str, checkpoint: bool = True) -> hl.Table:
    if checkpoint and (env == Env.LOCAL or env == Env.TEST):
        with tempfile.TemporaryDirectory() as d:
            ht = ht.checkpoint(
                os.path.join(
                    d,
                    f'{uuid.uuid4()}.ht',
                )
            )
            return ht.write(destination_path, overwrite=True, stage_locally=True)
    elif checkpoint:
        ht = ht.checkpoint(
            os.path.join(
                DataRoot.SEQR_SCRATCH_TEMP,
                f'{uuid.uuid4()}.ht',
            )
        )
    return ht.write(destination_path, overwrite=True, stage_locally=True)