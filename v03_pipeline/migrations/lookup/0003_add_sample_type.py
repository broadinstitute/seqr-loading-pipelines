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
        Adds sample_type to lookup ht global fields project_guids, project_families, and updates.
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
            'project_guids': array<tuple (
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
                remap_pedigree_hash: int32,
                sample_type: str
            }>
        """
        collected_globals = ht.globals.collect()[0]
        project_guids = collected_globals['project_guids']
        project_families = collected_globals['project_families']
        updates = collected_globals['updates']
        projects_without_hts = set()

        for i, project_guid in enumerate(project_guids):
            project_has_ht = False
            for sample_type in SampleType:
                project_ht_path = project_table_path(
                    reference_genome,
                    dataset_type,
                    sample_type,
                    project_guid,
                )
                if not hfs.exists(project_ht_path):
                    continue

                project_has_ht = True
                key = (project_guid, sample_type.value)
                project_guids[i] = key
                project_families[key] = project_families.pop(project_guid)
                for update in updates:
                    if update['project_guid'] == project_guid:
                        new_update = update.annotate(sample_type=sample_type.value)
                        updates.remove(update)
                        updates.add(new_update)
                break

            # It is possible that there are projects in the lookup globals with no corresponding project ht.
            # For those projects, set sample_type to missing and log project_guid, so we can delete them later.
            if not project_has_ht:
                projects_without_hts.add(project_guid)
                key = (project_guid, hl.missing(hl.tstr))
                project_guids[i] = key
                project_families[key] = project_families.pop(project_guid)
                for update in updates:
                    if update['project_guid'] == project_guid:
                        new_update = update.annotate(sample_type=hl.missing(hl.tstr))
                        updates.remove(update)
                        updates.add(new_update)

        if projects_without_hts:
            logger.info(f'Projects without hts: {projects_without_hts}')

        return ht.annotate_globals(
            project_guids=hl.array(project_guids),
            project_families=project_families,
            updates=updates,
        )
