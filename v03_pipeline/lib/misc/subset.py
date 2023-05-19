import hail as hl

from v03_pipeline.lib.misc.errors import MatrixTableSampleSetError


def subset_samples_and_variants(
    mt: hl.MatrixTable,
    sample_subset_ht: hl.Table,
    ignore_missing_samples: bool,
) -> hl.MatrixTable:
    """
    Subset the MatrixTable to the provided list of samples and to variants present in those samples
    :param mt: MatrixTable from VCF
    :param subset_path: Path to a file with a single column 's'
    :return: MatrixTable subsetted to list of samples
    """
    subset_count = sample_subset_ht.count()
    anti_join_ht = sample_subset_ht.anti_join(mt.cols())
    anti_join_ht_count = anti_join_ht.count()

    if anti_join_ht_count != 0:
        missing_samples = anti_join_ht.s.collect()
        message = (
            f'Only {subset_count - anti_join_ht_count} out of {subset_count} '
            f'subsetting-table IDs matched IDs in the variant callset.\n'
            f"IDs that aren't in the callset: {missing_samples}\n"
            f'All callset sample IDs:{mt.s.collect()}'
        )
        if (subset_count > anti_join_ht_count) and ignore_missing_samples:
            print(message)
        else:
            raise MatrixTableSampleSetError(message, missing_samples)

    mt = mt.semi_join_cols(sample_subset_ht)
    mt = mt.filter_rows(hl.agg.any(mt.GT.is_non_ref()))

    print(
        f'Finished subsetting samples. Kept {subset_count} '
        f'out of {mt.count()} samples in vds',
    )
    return mt
