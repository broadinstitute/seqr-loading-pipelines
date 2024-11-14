import hail as hl

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import vcf_to_ht


# adapted from download_and_create_reference_datasets/v02/hail_scripts/write_splice_ai.py
def get_ht(
    raw_dataset_paths: list[str],
    reference_genome: ReferenceGenome,
) -> hl.Table:
    ht = vcf_to_ht(raw_dataset_paths, reference_genome)

    # SpliceAI INFO field description from the VCF header:
    # SpliceAIv1.3 variant annotation. These include delta scores (DS) and delta positions (DP) for acceptor gain (AG),
    # acceptor loss (AL), donor gain (DG), and donor loss (DL).
    # Format: ALLELE|SYMBOL|DS_AG|DS_AL|DS_DG|DS_DL|DP_AG|DP_AL|DP_DG|DP_DL
    ds_start_index = 2
    ds_end_index = 6
    num_delta_scores = ds_end_index - ds_start_index
    ht = ht.select(
        delta_scores=ht.info.SpliceAI[0].split(delim="\\|")[ds_start_index:ds_end_index].map(hl.float32),
    )
    ht = ht.annotate(delta_score=hl.max(ht.delta_scores))
    # Splice Consequence enum ID is the index of the max score. If no score, use the last index for "No Consequence"
    return ht.annotate(
        splice_consequence_id=hl.if_else(ht.delta_score > 0, ht.delta_scores.index(ht.delta_score), num_delta_scores),
    ).drop('delta_scores')
