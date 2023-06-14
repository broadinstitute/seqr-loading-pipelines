import hail as hl


def _annotate_dict_expression(
    dict_expression: hl.DictExpression,
    key: hl.Expression,
    value: hl.Expression,
) -> hl.DictExpression:
    # Hail doesn't support adding or modifying a dictionary... so we make a new one from the values!
    items = hl.or_else(
        dict_expression.items().filter(lambda item: item[0] != key),
        hl.empty_array(hl.ttuple(key.dtype, value.dtype)),
    )
    return hl.dict(items.append((key, value)))


def compute_sample_lookup_ht(mt: hl.MatrixTable, project_guid: str) -> hl.Table:
    sample_ids = hl.agg.collect_as_set(mt.s)
    return mt.select_rows(
        ref_samples={project_guid: hl.agg.filter(mt.GT.is_hom_ref(), sample_ids)},
        het_samples={project_guid: hl.agg.filter(mt.GT.is_het(), sample_ids)},
        hom_samples={project_guid: hl.agg.filter(mt.GT.is_hom_var(), sample_ids)},
    ).rows()


def remove_callset_sample_ids(
    sample_lookup_ht: hl.Table,
    sample_subset_ht: hl.Table,
    project_guid: str,
) -> hl.Table:
    sample_ids = sample_subset_ht.aggregate(hl.agg.collect_as_set(sample_subset_ht.s))
    if hl.eval(~sample_lookup_ht.updates.project_guid.contains(project_guid)):
        return sample_lookup_ht
    project_guid_expression = hl.literal(project_guid)
    return sample_lookup_ht.select(
         ref_samples=_annotate_dict_expression(
             sample_lookup_ht.ref_samples,
             project_guid_expression,
             sample_lookup_ht.ref_samples[project_guid].difference(sample_ids),
         ),
         het_samples=_annotate_dict_expression(
             sample_lookup_ht.het_samples,
             project_guid_expression,
             sample_lookup_ht.het_samples[project_guid].difference(sample_ids),
         ),
         hom_samples=_annotate_dict_expression(
             sample_lookup_ht.hom_samples,
             project_guid_expression,
             sample_lookup_ht.hom_samples[project_guid].difference(sample_ids),
         ),
     )


def union_sample_lookup_hts(
    sample_lookup_ht: hl.Table,
    callset_sample_lookup_ht: hl.Table,
    project_guid: str,
) -> hl.Table:
    sample_lookup_ht = sample_lookup_ht.join(callset_sample_lookup_ht, 'outer')
    project_guid_expression = hl.literal(project_guid)
    return sample_lookup_ht.select(
        ref_samples=_annotate_dict_expression(
            sample_lookup_ht.ref_samples,
            project_guid_expression,
            sample_lookup_ht.ref_samples[project_guid].union(
                sample_lookup_ht.ref_samples_1[project_guid],
            ),
        ),
        het_samples=_annotate_dict_expression(
            sample_lookup_ht.het_samples,
            project_guid_expression,
            sample_lookup_ht.het_samples[project_guid].union(
                sample_lookup_ht.het_samples_1[project_guid],
            ),
        ),
        hom_samples=_annotate_dict_expression(
            sample_lookup_ht.hom_samples,
            project_guid_expression,
            sample_lookup_ht.hom_samples[project_guid].union(
                sample_lookup_ht.hom_samples_1[project_guid],
            ),
        ),
    )
