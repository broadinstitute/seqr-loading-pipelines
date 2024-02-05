import hail as hl

from v03_pipeline.lib.misc.io import import_pedigree
from v03_pipeline.lib.misc.pedigree import parse_pedigree_ht_to_families
from v03_pipeline.lib.misc.sample_entries import (
    deglobalize_sample_ids,
)

# Need to use the GCP bucket as temp storage for very large callset joins
hl.init(tmp_dir='gs://seqr-scratch-temp', idempotent=True)

# Interval ref data join causes shuffle death, this prevents it
hl._set_flags(use_new_shuffle='1', no_whole_stage_codegen='1')  # noqa: SLF001
sc = hl.spark_context()
sc.addPyFile('gs://seqr-luigi/releases/dev/latest/pyscripts.zip')


pedigree_ht = import_pedigree('gs://seqr-datasets/v02/GRCh38/RDG_WGS_Broad_Internal/base/projects/R0384_rare_genomes_project_gen/R0384_rare_genomes_project_gen_pedigree.tsv')
families = parse_pedigree_ht_to_families(pedigree_ht)
sample_id_to_family_guid = hl.dict({
    s: f.family_guid
    for f in families
    for s in f.samples
})

ht = hl.read_table('gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/projects/R0384_rare_genomes_project_gen.ht')
ht = deglobalize_sample_ids(ht)
ht = ht.select(
    filters=ht.filters,
    family_entries = ht.entries.map(
        lambda x: (
            x.annotate(
                f=sample_id_to_family_guid[x.s],
            )
        ),
    )
    .group_by(lambda x: x.f)
    .values()
    .map(
        lambda x: hl.sorted(x, key=lambda x: x.s),
    ),
)

row = ht.take(1)
ht = ht.annotate_globals(
    family_guids=(
        [fe[0].f for fe in row[0].family_entries]
        if (row and len(row[0].family_entries) > 0)
        else hl.empty_array(hl.tstr)
    ),
    family_samples=(
        {
            fe[0].f: [e.s for e in fe] for fe in row[0].family_entries
        }
        if (row and len(row[0].family_entries) > 0)
        else hl.empty_dict(hl.tstr, hl.tarray(hl.tstr))
    ),
)
ht = ht.annotate(family_entries=ht.family_entries.map(lambda fe: fe.map(lambda e: e.drop('s', 'f'))))
ht = ht.annotate(family_entries=ht.family_entries.map(lambda fe: hl.or_missing(fe.any(lambda e: e.GT.is_non_ref()), fe)))
ht.write('gs://seqr-scratch-temp/R0384_rare_genomes_project_gen_family_entries_repartitioned.ht', overwrite=True)


#5.54 GiB     gs://seqr-scratch-temp/R0384_rare_genomes_project_gen_family_entries_repartitioned_rare_10_percent.ht/
#23.15 GiB    gs://seqr-scratch-temp/R0384_rare_genomes_project_gen_family_entries_repartitioned_common_10_percent.ht/
#4.27 GiB     gs://seqr-scratch-temp/R0384_rare_genomes_project_gen_family_entries_repartitioned_rare_5_percent.ht/
#
