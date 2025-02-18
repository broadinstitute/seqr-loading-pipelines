import hail as hl

from v03_pipeline.lib.annotations import sv
from v03_pipeline.lib.misc.pedigree import Family
from v03_pipeline.lib.model import ReferenceGenome, Sex


def overwrite_male_non_par_calls(
    mt: hl.MatrixTable,
    families: set[Family],
) -> hl.MatrixTable:
    male_sample_ids = {
        s.sample_id for f in families for s in f.samples.values() if s.sex == Sex.MALE
    }
    male_sample_ids = (
        hl.set(male_sample_ids) if male_sample_ids else hl.empty_set(hl.str)
    )
    par_intervals = hl.array(
        [
            i
            for i in hl.get_reference(ReferenceGenome.GRCh38).par
            if i.start.contig == ReferenceGenome.GRCh38.x_contig
        ],
    )
    non_par_interval = hl.interval(
        par_intervals[0].end,
        par_intervals[1].start,
    )
    # NB: making use of existing formatting_annotation_fns.
    # We choose to annotate & drop here as the sample level
    # fields are dropped by the time we format variants.
    mt = mt.annotate_rows(
        start_locus=sv.start_locus(mt),
        end_locus=sv.end_locus(mt),
    )
    mt = mt.annotate_entries(
        GT=hl.if_else(
            (
                male_sample_ids.contains(mt.s)
                & non_par_interval.overlaps(
                    hl.interval(
                        mt.start_locus,
                        mt.end_locus,
                    ),
                )
                & mt.GT.is_het()
            ),
            hl.Call([1], phased=False),
            mt.GT,
        ),
    )
    return mt.drop('start_locus', 'end_locus')
