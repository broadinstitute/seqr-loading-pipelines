import hail as hl

from v03_pipeline.lib.model import ReferenceGenome


# adapted from download_and_create_reference_datasets/v02/hail_scripts/write_splice_ai.py
def get_ht(
    raw_dataset_paths: list[str],
    reference_genome: ReferenceGenome,
) -> hl.Table:
    ht = hl.import_vcf(
        raw_dataset_paths,
        rreference_genome=reference_genome.value,
        drop_samples=True,
        skip_invalid_loci=True,
        contig_recoding=reference_genome.contig_recoding(include_mt=True),
        force_bgz=True,
    ).rows()

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