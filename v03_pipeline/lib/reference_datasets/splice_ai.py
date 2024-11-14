import hail as hl

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import vcf_to_ht


# adapted from download_and_create_reference_datasets/v02/hail_scripts/write_splice_ai.py
def get_ht(
    raw_dataset_paths: list[str],
    reference_genome: ReferenceGenome,
) -> hl.Table:
    ht = vcf_to_ht(raw_dataset_paths, reference_genome)

    # TODO
    delta_scores = ht.info.SpliceAI[0].split(delim="\\|")[2:6]
    splice_split = ht.info.annotate(
        SpliceAI=hl.map(lambda x: hl.float32(x), delta_scores)
    )
    ht = ht.annotate(info=splice_split)

    # Annotate info.max_DS with the max of DS_AG, DS_AL, DS_DG, DS_DL in info.
    # delta_score array is |DS_AG|DS_AL|DS_DG|DS_DL
    consequences = hl.literal(
        ["Acceptor gain", "Acceptor loss", "Donor gain", "Donor loss"]
    )
    ht = ht.annotate(info=ht.info.annotate(max_DS=hl.max(ht.info.SpliceAI)))
    ht = ht.annotate(
        info=ht.info.annotate(
            splice_consequence=hl.if_else(
                ht.info.max_DS > 0,
                consequences[ht.info.SpliceAI.index(ht.info.max_DS)],
                "No consequence",
            )
        )
    )

"""
'select': {
                'delta_score': 'info.max_DS',
                'splice_consequence': 'info.splice_consequence',
            },
            
          ENUMS:  'splice_consequence': [
                'Acceptor gain',
                'Acceptor loss',
                'Donor gain',
                'Donor loss',
                'No consequence',
            ],
"""