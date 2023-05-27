from __future__ import annotations

from typing import TYPE_CHECKING

import luigi_pipeline.lib.hail_vep_runners as vep_runners
from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome

if TYPE_CHECKING:
    import hail as hl


def run_vep(
    mt: hl.Table | hl.MatrixTable,  # TODO: use a TypeVar
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    vep_config_json_path: str | None,
) -> hl.Table | hl.MatrixTable:
    if hasattr(mt, 'vep') or not dataset_type.veppable:
        return mt
    vep_runner = (
        vep_runners.HailVEPRunner()
        if env != Env.TEST
        else vep_runners.HailVEPDummyRunner()
    )
    return vep_runner.run(
        mt,
        reference_genome.v02_value,
        vep_config_json_path=vep_config_json_path,
    )
