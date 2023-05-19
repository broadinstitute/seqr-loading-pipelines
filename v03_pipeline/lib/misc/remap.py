from collections import Counter

import hail as hl

from v03_pipeline.lib.misc.errors import MatrixTableSampleSetError


def vcf_remap(mt: hl.MatrixTable) -> hl.MatrixTable:
    # TODO: add logic from Mike to remap vcf samples delivered from Broad WGS
    pass


def remap_sample_ids(mt: hl.MatrixTable, project_remap_ht: hl.Table) -> hl.MatrixTable:
    """
    Remap the MatrixTable's sample ID, 's', field to the sample ID used within seqr, 'seqr_id'
    If the sample 's' does not have a 'seqr_id' in the remap file, 's' becomes 'seqr_id'
    :param mt: MatrixTable from VCF
    :param remap_path: Path to a file with two columns 's' and 'seqr_id'
    :return: MatrixTable remapped and keyed to use seqr_id
    """
    mt = vcf_remap(mt)
    s_dups = [k for k, v in Counter(project_remap_ht.s.collect()).items() if v > 1]
    seqr_dups = [
        k for k, v in Counter(project_remap_ht.seqr_id.collect()).items() if v > 1
    ]

    if len(s_dups) > 0 or len(seqr_dups) > 0:
        msg = f'Duplicate s or seqr_id entries in remap file were found. Duplicate s:{s_dups}. Duplicate seqr_id:{seqr_dups}.'
        raise ValueError(msg)

    missing_samples = project_remap_ht.anti_join(mt.cols()).collect()
    remap_count = project_remap_ht.count()

    if len(missing_samples) != 0:
        msg = f'Only {project_remap_ht.semi_join(mt.cols()).count()} out of {remap_count} '
        'remap IDs matched IDs in the variant callset.\n'
        f"IDs that aren't in the callset: {missing_samples}\n"
        f'All callset sample IDs:{mt.s.collect()}'
        raise MatrixTableSampleSetError(msg, missing_samples)

    mt = mt.annotate_cols(**project_remap_ht[mt.s])
    remap_expr = hl.cond(hl.is_missing(mt.seqr_id), mt.s, mt.seqr_id)
    mt = mt.annotate_cols(seqr_id=remap_expr, vcf_id=mt.s)
    mt = mt.key_cols_by(s=mt.seqr_id)
    print(f'Remapped {remap_count} sample ids...')
    return mt
