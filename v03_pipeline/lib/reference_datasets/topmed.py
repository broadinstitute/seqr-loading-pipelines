import hail as hl

from v03_pipeline.lib.model import ReferenceGenome

RENAME = {
    'info.AC#': 'AC',
    'info.AF#': 'AF',
    'info.AN': 'AN',
    'info.Hom#': 'Hom',
    'info.Het#': 'Het',
}

# adapted from download_and_create_reference_datasets/v02/create_ht__topmed.py
def get_ht(raw_dataset_path: str, reference_genome: ReferenceGenome) -> hl.Table:
    ht = hl.import_vcf(
        raw_dataset_path,
        rreference_genome=reference_genome.value,
        drop_samples=True,
        skip_invalid_loci=True,
        contig_recoding=reference_genome.contig_recoding(include_mt=True),
        force_bgz=True,
    ).rows()
    return ht.rename(**RENAME).select(*RENAME.values())
