import hail as hl
import hailtop.fs as hfs

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.migration.base_migration import BaseMigration
from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.paths import project_table_path

logger = get_logger(__name__)


class AddLookupSampleType(BaseMigration):
    reference_genome_dataset_types: frozenset[
        tuple[ReferenceGenome, DatasetType]
    ] = frozenset(
        (
            (ReferenceGenome.GRCh37, DatasetType.SNV_INDEL),
            (ReferenceGenome.GRCh38, DatasetType.SNV_INDEL),
            (ReferenceGenome.GRCh38, DatasetType.MITO),
        ),
    )

    @staticmethod
    def migrate(
        ht: hl.Table,
        reference_genome: ReferenceGenome,
        dataset_type: DatasetType,
    ) -> hl.Table:
        """
        Renames project_guids to project_sample_types.
        Adds sample_type to global fields project_sample_types and project_families.
        Assumes that only one project_ht exists for each project across both sample types.

        Old Global fields:
            'project_guids': array<str>
            'project_families': dict<str, array<str>>
            'updates': set<struct {
                callset: str,
                project_guid: str,
                remap_pedigree_hash: int32
            }>
        New Global fields:
            'project_sample_types': array<tuple (
                str,
                str
            )>
            'project_families': dict<tuple (
                str,
                str
            ), array<str>>
            'updates': set<struct {
                callset: str,
                project_guid: str,
                remap_pedigree_hash: int32
            }>
        """
        ht = ht.transmute_globals(
            project_sample_types=ht.globals.project_guids,
        )
        collected_globals = ht.globals.collect()[0]
        project_sample_types = collected_globals['project_sample_types']
        project_families = collected_globals['project_families']

        for i, project_guid in enumerate(project_sample_types):
            for sample_type in SampleType:
                project_ht_path = project_table_path(
                    reference_genome,
                    dataset_type,
                    sample_type,
                    project_guid,
                )
                if not hfs.exists(project_ht_path):
                    continue

                key = (project_guid, sample_type.value)
                project_sample_types[i] = key
                project_families[key] = project_families.pop(project_guid)
                break

        return ht.annotate_globals(
            project_sample_types=project_sample_types,
            project_families=project_families,
        )
