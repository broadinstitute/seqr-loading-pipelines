from __future__ import annotations

import os
import tempfile
import uuid

import hail as hl

from v03_pipeline.lib.definitions import DataRoot, Env


def import_pedigree(pedigree_path: str) -> hl.Table:
    ht = hl.import_table(pedigree_path)
    ht = ht.select(
        family_id=ht.Family_ID,
        s=ht.Individual_ID,
    )
    return ht.key_by(ht.family_id)


def write_ht(
    env: Env,
    ht: hl.Table,
    destination_path: str,
    checkpoint: bool = True,
) -> hl.Table:
    if checkpoint and (env == Env.LOCAL or env == Env.TEST):
        with tempfile.TemporaryDirectory() as d:
            ht = ht.checkpoint(
                os.path.join(
                    d,
                    f'{uuid.uuid4()}.ht',
                ),
            )
            return ht.write(destination_path, overwrite=True, stage_locally=True)
    elif checkpoint:
        ht = ht.checkpoint(
            os.path.join(
                DataRoot.SEQR_SCRATCH_TEMP,
                f'{uuid.uuid4()}.ht',
            ),
        )
    return ht.write(destination_path, overwrite=True, stage_locally=True)
