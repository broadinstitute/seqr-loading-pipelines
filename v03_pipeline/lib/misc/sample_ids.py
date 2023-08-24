from collections import Counter

import hail as hl


class MatrixTableSampleSetError(Exception):
    def __init__(self, message, missing_samples):
        super().__init__(message)
        self.missing_samples = missing_samples


def vcf_remap(mt: hl.MatrixTable) -> hl.MatrixTable:
    # TODO: add logic from Mike to remap vcf samples delivered from Broad WGS
    return mt


def remap_sample_ids(
    mt: hl.MatrixTable,
    project_remap_ht: hl.Table,
    ignore_missing_samples_when_remapping: bool,
) -> hl.MatrixTable:
    mt = vcf_remap(mt)
    collected_remap = project_remap_ht.collect()
    s_dups = [k for k, v in Counter([r.s for r in collected_remap]).items() if v > 1]
    seqr_dups = [
        k for k, v in Counter([r.seqr_id for r in collected_remap]).items() if v > 1
    ]

    if len(s_dups) > 0 or len(seqr_dups) > 0:
        msg = f'Duplicate s or seqr_id entries in remap file were found. Duplicate s:{s_dups}. Duplicate seqr_id:{seqr_dups}.'
        raise ValueError(msg)

    missing_samples = project_remap_ht.anti_join(mt.cols()).collect()
    remap_count = len(collected_remap)

    if len(missing_samples) != 0:
        message = (
            f'Only {project_remap_ht.semi_join(mt.cols()).count()} out of {remap_count} '
            'remap IDs matched IDs in the variant callset.\n'
            f"IDs that aren't in the callset: {missing_samples}\n"
            f'All callset sample IDs:{mt.s.collect()}'
        )
        if ignore_missing_samples_when_remapping:
            print(message)
        else:
            raise MatrixTableSampleSetError(message, missing_samples)

    mt = mt.annotate_cols(**project_remap_ht[mt.s])
    remap_expr = hl.cond(hl.is_missing(mt.seqr_id), mt.s, mt.seqr_id)
    mt = mt.annotate_cols(seqr_id=remap_expr, vcf_id=mt.s)
    mt = mt.key_cols_by(s=mt.seqr_id)
    print(f'Remapped {remap_count} sample ids...')
    return mt


def subset_samples(
    mt: hl.MatrixTable,
    sample_subset_ht: hl.Table,
    ignore_missing_samples_when_subsetting: bool,
) -> hl.MatrixTable:
    subset_count = sample_subset_ht.count()
    anti_join_ht = sample_subset_ht.anti_join(mt.cols())
    anti_join_ht_count = anti_join_ht.count()
    if subset_count == 0:
        message = '0 sample ids found the subset HT, something is probably wrong.'
        raise MatrixTableSampleSetError(message, [])

    if anti_join_ht_count != 0:
        missing_samples = anti_join_ht.s.collect()
        message = (
            f'Only {subset_count - anti_join_ht_count} out of {subset_count} '
            f'subsetting-table IDs matched IDs in the variant callset.\n'
            f"IDs that aren't in the callset: {missing_samples}\n"
            f'All callset sample IDs:{mt.s.collect()}'
        )
        if (subset_count > anti_join_ht_count) and ignore_missing_samples_when_subsetting:
            print(message)
        else:
            raise MatrixTableSampleSetError(message, missing_samples)
    print(f'Subsetted to {subset_count} sample ids')
    return mt.semi_join_cols(sample_subset_ht)
