import hail as hl
import requests

from v03_pipeline.lib.model.dataset_type import DatasetType
from v03_pipeline.lib.model.definitions import ReferenceGenome


def get_ht(
    url: str,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    response = requests.get(url, stream=True, timeout=10)
    data = response.json()
    ht = hl.Table.parallelize(data)
    ht = ht.select(
        locus=hl.locus(
            reference_genome.mito_contig,
            ht.nt_start,
            reference_genome.value,
        ),
        alleles=hl.array([ht.ref_rCRS, ht.alt]),
        score=ht.disease_score,
    )
    ht = ht.key_by('locus', 'alleles')
    ht = ht.filter(
        ~DatasetType.SNV_INDEL.invalid_allele_types.contains(
            hl.numeric_allele_type(ht.alleles[0], ht.alleles[1]),
        ),
    )
    return ht.group_by(*ht.key).aggregate(score=hl.agg.max(ht.score))
