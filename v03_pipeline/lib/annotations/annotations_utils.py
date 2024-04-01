import hail as hl

from hail_scripts.utils.mapping_gene_ids import load_gencode
from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import (
    lookup_table_path,
    valid_reference_dataset_collection_path,
)

GENCODE_RELEASE = 42


def get_annotation_dependencies(
    dataset_type: DatasetType,
    reference_genome: ReferenceGenome,
) -> dict[str, hl.Table]:
    deps = {}
    for rdc in ReferenceDatasetCollection.for_reference_genome_dataset_type(
        reference_genome,
        dataset_type,
    ):
        deps[f'{rdc.value}_ht'] = hl.read_table(
            valid_reference_dataset_collection_path(
                reference_genome,
                dataset_type,
                rdc,
            ),
        )
    if dataset_type.has_lookup_table:
        deps['lookup_ht'] = hl.read_table(
            lookup_table_path(
                reference_genome,
                dataset_type,
            ),
        )
    if dataset_type.has_gencode_mapping:
        deps['gencode_mapping'] = hl.literal(
            load_gencode(GENCODE_RELEASE, ''),
        )
    return deps
