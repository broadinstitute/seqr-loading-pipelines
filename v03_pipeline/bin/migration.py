import subprocess

import hail as hl

from v03_pipeline.lib.misc.family_entries import globalize_ids
from v03_pipeline.lib.misc.io import import_pedigree, write
from v03_pipeline.lib.misc.pedigree import parse_pedigree_ht_to_families
from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome

# Need to use the GCP bucket as temp storage for very large callset joins
hl.init(tmp_dir='gs://seqr-scratch-temp', idempotent=True)

# Interval ref data join causes shuffle death, this prevents it
hl._set_flags(use_new_shuffle='1', no_whole_stage_codegen='1')  # noqa: SLF001
sc = hl.spark_context()
sc.addPyFile('gs://seqr-luigi/releases/dev/latest/pyscripts.zip')


def deglobalize_sample_ids(ht: hl.Table) -> hl.Table:
    ht = ht.annotate(
        entries=(
            hl.enumerate(ht.entries).starmap(
                lambda i, e: hl.Struct(**e, s=ht.sample_ids[i]),
            )
        ),
    )
    return ht.drop('sample_ids')


Env.HAIL_TMPDIR = 'gs://seqr-scratch-temp'

MIGRATIONS = [
    (
        DatasetType.SNV_INDEL,
        ReferenceGenome.GRCh37,
    ),
    (
        DatasetType.SNV_INDEL,
        ReferenceGenome.GRCh38,
    ),
    (
        DatasetType.MITO,
        ReferenceGenome.GRCh38,
    ),
    (
        DatasetType.SV,
        ReferenceGenome.GRCh38,
    ),
    (
        DatasetType.GCNV,
        ReferenceGenome.GRCh38,
    ),
]


for dataset_type, reference_genome in MIGRATIONS:
    projects = []
    path = f"gs://seqr-hail-search-data/v03/{reference_genome.value}/{dataset_type.value}/projects/"
    r = subprocess.run(
         ["gsutil", "ls", path],
         stdout=subprocess.PIPE,
         stderr=subprocess.PIPE,
         universal_newlines=True,
     )
    if "matched no objects" in r.stderr:
        continue
    for line in r.stdout.strip().split("\n"):
        if line.endswith('projects/'):
            continue
        projects.append(line)
    for project_table_path in projects:
        print(project_table_path)
        ht = hl.read_table(project_table_path)
        if hasattr(ht, 'family_entries'):
            continue
        sample_type = hl.eval(ht.globals.sample_type)

        project_name = project_table_path.replace('.ht/', '').split('/')[-1]
        try:
            if reference_genome == ReferenceGenome.GRCh38:
                pedigree_ht = import_pedigree(
                    f'gs://seqr-datasets/v02/{reference_genome.value}/RDG_{sample_type}_Broad_Internal/base/projects/{project_name}/{project_name}_pedigree.tsv',
                )
            else:
                pedigree_ht = import_pedigree(
                    f'gs://seqr-datasets/v02/{reference_genome.value}/RDG_{sample_type}_Broad_External/base/projects/{project_name}/{project_name}_pedigree.tsv',
                )
        except Exception:  # noqa: BLE001
            pedigree_ht = import_pedigree(
                f'gs://seqr-datasets/v02/{reference_genome.value}/AnVIL_{sample_type}/{project_name}/base/{project_name}_pedigree.tsv',
            )

        families = parse_pedigree_ht_to_families(pedigree_ht)
        sample_id_to_family_guid = hl.dict(
            {s: f.family_guid for f in families for s in f.samples},
        )
        ht = deglobalize_sample_ids(ht)
        ht = ht.select(
            filters=ht.filters,
            family_entries=hl.sorted(
                ht.entries.map(
                    lambda x: (
                        x.annotate(
                            family_guid=sample_id_to_family_guid[x.s], # noqa: B023
                        )
                    ),
                )
                .group_by(lambda x: x.family_guid)
                .values()
                .map(
                    lambda x: hl.sorted(x, key=lambda x: x.s),
                ),
                lambda fe: fe[0].family_guid,
            ),
        )
        ht = globalize_ids(ht)
        ht = ht.annotate(
            family_entries=(
                ht.family_entries.map(
                    lambda fe: hl.or_missing(
                        fe.any(dataset_type.family_entries_filter_fn), # noqa: B023
                        fe,
                    ),
                )
            ),
        )
        ht.filter(ht.family_entries.any(hl.is_defined))
        write(ht, project_table_path)
