import subprocess

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType

MIGRATIONS = [
    (
        DatasetType.GCNV,
        ReferenceGenome.GRCh38,
    ),
    (
        DatasetType.SV,
        ReferenceGenome.GRCh38,
    ),
    (
        DatasetType.MITO,
        ReferenceGenome.GRCh38,
    ),
    (
        DatasetType.SNV_INDEL,
        ReferenceGenome.GRCh37,
    ),
    (
        DatasetType.SNV_INDEL,
        ReferenceGenome.GRCh38,
    ),
]

PROJECT_LOOKUP = dict(x.split('\t') for x in open('project_sample_types.txt').read().split('\n') if x)
FAMILY_LOOKUP = dict(x.split('\t') for x in open('family_sample_types.txt').read().split('\n') if x)

for dataset_type, reference_genome in MIGRATIONS:
    if dataset_type == DatasetType.MITO or dataset_type == DatasetType.SV:
        sample_type = SampleType.WGS
    elif dataset_type == DatasetType.GCNV:
        sample_type = SampleType.WES
    else:
        sample_type = None

    # Projects
    project_guids = []
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
        project_guids.append(line.split('/')[-2].replace('.ht',''))
    for project_guid in project_guids:
        try:
            project_sample_type = sample_type if sample_type else SampleType(PROJECT_LOOKUP[project_guid])
        except KeyError:
            print('Skipping Project Guid', project_guid)
            continue
        #r = subprocess.run(
        #    ['gsutil', 'cp', '-r' , f'{path}{project_guid}.ht', f'gs://seqr-hail-search-data/v3.1/{reference_genome.value}/{dataset_type.value}/projects/{project_sample_type.value}/{project_guid}.ht'],
        #    capture_output=True,
        #    text=True,
        #    check=True,
        #)
        print(['gsutil', 'cp', '-r' , f'{path}{project_guid}.ht', f'gs://seqr-hail-search-data/v3.1/{reference_genome.value}/{dataset_type.value}/projects/{project_sample_type.value}/{project_guid}.ht'])


    # Families
    family_guids = []
    path = f'gs://seqr-hail-search-data/v03/{reference_genome.value}/{dataset_type.value}/families/'
    r = subprocess.run(
        ['gsutil', 'ls', path],
        capture_output=True,
        text=True,
        check=True,
    )
    if 'matched no objects' in r.stderr:
         continue
    for line in r.stdout.strip().split('\n'):
        if line.endswith('families/'):
            continue
        family_guids.append(line.split('/')[-2].replace('.ht',''))
    for family_guid in family_guids:
        try:
            family_sample_type = sample_type if sample_type else SampleType(FAMILY_LOOKUP[family_guid])
        except KeyError:
            print('Skipping Family Guid', family_guid)
            continue
        #r = subprocess.run(
        #    ['gsutil', 'cp', '-r' , f'{path}{family_guid}.ht', f'gs://seqr-hail-search-data/v3.1/{reference_genome.value}/{dataset_type.value}/families/{family_sample_type.value}/{family_guid}.ht'],
        #    capture_output=True,
        #    text=True,
        #    check=True,
        #)
        print(['gsutil', 'cp', '-r' , f'{path}{family_guid}.ht', f'gs://seqr-hail-search-data/v3.1/{reference_genome.value}/{dataset_type.value}/families/{family_sample_type.value}/{family_guid}.ht'])
