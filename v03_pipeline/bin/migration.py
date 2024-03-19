import json
import os
import subprocess

import hail as hl

from v03_pipeline.lib.misc.lookup import join_lookup_hts
from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.model.constants import PROJECTS_EXCLUDED_FROM_LOOKUP

# Need to use the GCP bucket as temp storage for very large callset joins
hl.init(tmp_dir='gs://seqr-scratch-temp', idempotent=True)

# Interval ref data join causes shuffle death, this prevents it
hl._set_flags(use_new_shuffle='1', no_whole_stage_codegen='1')  # noqa: SLF001
sc = hl.spark_context()
sc.addPyFile('gs://seqr-luigi/releases/dev/latest/pyscripts.zip')

Env.HAIL_TMPDIR = 'gs://seqr-scratch-temp'

MIGRATIONS = [
    #(
    #    DatasetType.MITO,
    #    ReferenceGenome.GRCh38,
    #),
    (
        DatasetType.SNV_INDEL,
        ReferenceGenome.GRCh37,
    ),
    #(
    #    DatasetType.SNV_INDEL,
    #    ReferenceGenome.GRCh38,
    #),
]

def build_sample_id_to_family_guid():
    sample_id_to_family_guid = {}
    for dataset_type, reference_genome in MIGRATIONS:
        projects = []
        path = f'gs://seqr-hail-search-data/v03/{reference_genome.value}/{dataset_type.value}/projects/'
        r = subprocess.run(
            ['gsutil', 'ls', path],
            capture_output=True,
            text=True,
            check=True,
        )
        if 'matched no objects' in r.stderr:
            continue
        for line in r.stdout.strip().split('\n'):
            if line.endswith('projects/'):
                continue
            projects.append(line)
        for project_table_path in projects:
            print(project_table_path)
            ht = hl.read_table(project_table_path)
            project_guid = project_table_path.replace('.ht/', '').split('/')[-1]
            if project_guid in sample_id_to_family_guid:
                sample_id_to_family_guid[project_guid] = {
                    **{sample_id: family_guid for family_guid, samples in hl.eval(ht.globals.family_samples).items() for sample_id in samples},
                    **sample_id_to_family_guid[project_guid]
                }
            else:
                sample_id_to_family_guid = {
                    project_guid: {sample_id: family_guid for family_guid, samples in hl.eval(ht.globals.family_samples).items() for sample_id in samples},
                    **sample_id_to_family_guid
                }
    with open('sample_id_to_family_guid.json', 'w') as f:
        json.dump(sample_id_to_family_guid, f, ensure_ascii=False, indent=4)

    return sample_id_to_family_guid

def initialize_table(dataset_type: DatasetType, reference_genome: ReferenceGenome) -> hl.Table:
    key_type = dataset_type.table_key_type(reference_genome)
    return hl.Table.parallelize(
        [],
        hl.tstruct(
            **key_type,
            project_stats=hl.tarray(
                hl.tarray(
                    hl.tstruct(
                        **{
                            field: hl.tint32
                            for field in dataset_type.lookup_table_fields_and_genotype_filter_fns
                        },
                    ),
                ),
            ),
        ),
        key=key_type.fields,
        globals=hl.Struct(
            project_guids=hl.empty_array(hl.tstr),
            project_families=hl.empty_dict(hl.tstr, hl.tarray(hl.tstr)),
            updates=hl.empty_set(hl.tstruct(callset=hl.tstr, project_guid=hl.tstr)),
        ),
    )


sample_id_to_family_guid = build_sample_id_to_family_guid()
for dataset_type, reference_genome in MIGRATIONS:
    ht = initialize_table(dataset_type, reference_genome)
    sample_lookup_ht = hl.read_table(f'gs://seqr-hail-search-data/v03/{reference_genome.value}/{dataset_type.value}/lookup.ht')
    sample_lookup_ht = sample_lookup_ht.repartition(400)
    sample_lookup_ht = sample_lookup_ht.checkpoint('gs://seqr-scratch-temp/asdlkfj0.ht')
    for project_guid in sample_lookup_ht.ref_samples:
        if project_guid in PROJECTS_EXCLUDED_FROM_LOOKUP:
            continue
        if project_guid not in sample_id_to_family_guid:
            print('Skipping', project_guid)
            continue
        print(project_guid)
        if dataset_type == DatasetType.MITO:
            project_lookup_ht = sample_lookup_ht.select(
                ref_samples=hl.array(sample_lookup_ht.ref_samples[project_guid]).map(lambda sample_id: hl.dict(sample_id_to_family_guid[project_guid])[sample_id]),
                heteroplasmic_samples=hl.array(sample_lookup_ht.heteroplasmic_samples[project_guid]).map(lambda sample_id: hl.dict(sample_id_to_family_guid[project_guid])[sample_id]),
                homoplasmic_samples=hl.array(sample_lookup_ht.homoplasmic_samples[project_guid]).map(lambda sample_id: hl.dict(sample_id_to_family_guid[project_guid])[sample_id]),
                family_guids=hl.sorted(set(sample_id_to_family_guid[project_guid].values())),
            )
            project_lookup_ht = project_lookup_ht.select(
                project_stats = project_lookup_ht.family_guids.map(
                    lambda family_guid: hl.Struct(
                        ref_samples=hl.len(project_lookup_ht.ref_samples.filter(lambda f: f == family_guid)), 
                        heteroplasmic_samples=hl.len(project_lookup_ht.heteroplasmic_samples.filter(lambda f: f == family_guid)), 
                        homoplasmic_samples=hl.len(project_lookup_ht.homoplasmic_samples.filter(lambda f: f == family_guid)),
                    ),
                ),
                family_guids=project_lookup_ht.family_guids,
            )
            project_lookup_ht = project_lookup_ht.annotate(
                project_stats=[
                    # Set a family to missing if all values are 0
                    project_lookup_ht.project_stats.map(
                        lambda ps: hl.or_missing(
                            hl.sum(list(ps.values())) > 0,
                            ps,
                        ),
                    ),
                ],
            )
        else:
            project_lookup_ht = sample_lookup_ht.select(
                ref_samples=hl.array(sample_lookup_ht.ref_samples[project_guid]).map(lambda sample_id: hl.dict(sample_id_to_family_guid[project_guid])[sample_id]),
                het_samples=hl.array(sample_lookup_ht.het_samples[project_guid]).map(lambda sample_id: hl.dict(sample_id_to_family_guid[project_guid])[sample_id]),
                hom_samples=hl.array(sample_lookup_ht.hom_samples[project_guid]).map(lambda sample_id: hl.dict(sample_id_to_family_guid[project_guid])[sample_id]),
                family_guids=hl.sorted(set(sample_id_to_family_guid[project_guid].values())),
            )
            project_lookup_ht = project_lookup_ht.select(
                project_stats=project_lookup_ht.family_guids.map(
                    lambda family_guid: hl.Struct(
                        ref_samples=hl.len(project_lookup_ht.ref_samples.filter(lambda f: f == family_guid)), 
                        het_samples=hl.len(project_lookup_ht.het_samples.filter(lambda f: f == family_guid)), 
                        hom_samples=hl.len(project_lookup_ht.hom_samples.filter(lambda f: f == family_guid)),
                    )
                ),
                family_guids=project_lookup_ht.family_guids,
            )
            project_lookup_ht = project_lookup_ht.annotate(
                project_stats=[
                    # Set a family to missing if all values are 0
                    project_lookup_ht.project_stats.map(
                        lambda ps: hl.or_missing(
                            hl.sum(list(ps.values())) > 0,
                            ps,
                        ),
                    ),
                ],
            )
        family_guids = project_lookup_ht.family_guids.take(1)
        project_lookup_ht = project_lookup_ht.select_globals(
            project_guids=[project_guid],
            project_families={project_guid: family_guids[0]},
        )
        project_lookup_ht = project_lookup_ht.select('project_stats')
        ht = join_lookup_hts(ht, project_lookup_ht)
    ht = ht.annotate_globals(
        updates=sample_lookup_ht.index_globals().updates
    )
    ht.write('gs://seqr-scratch-temp/37_new_lookup.ht', overwrite=True)


