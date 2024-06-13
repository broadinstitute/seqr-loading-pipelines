import os
import hail as hl

# Need to use the GCP bucket as temp storage for very large callset joins
hl.init(tmp_dir='gs://seqr-scratch-temp', idempotent=True)

# Interval ref data join causes shuffle death, this prevents it
hl._set_flags(use_new_shuffle='1', no_whole_stage_codegen='1')
sc = hl.spark_context()
sc.addPyFile('gs://seqr-luigi/releases/dev/latest/pyscripts.zip')

from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.annotations import snv_indel, misc
from v03_pipeline.lib.reference_data.gencode.mapping_gene_ids import load_gencode_ensembl_to_refseq_id


gencode_ensembl_to_refseq_id_mapping = hl.literal(load_gencode_ensembl_to_refseq_id(44))
ht = hl.read_table('gs://seqr-hail-search-data/v03/GRCh38/SNV_INDEL/annotations.ht')
ht = ht.repartition(2500)
ht = ht.write('gs://seqr-scratch-temp/annotations_repartitioned.ht')
ht = hl.vep(
    ht,
    config=os.environ['VEP_CONFIG_URI'],
    name='vep',
    block_size=1000,
    tolerate_parse_error=True,
    csq=False,
)
ht = ht.write('gs://seqr-scratch-temp/annotations_repartitioned_vep.ht')
ht = ht.annotate(
    check_ref=snv_indel.check_ref(ht),
    sorted_transcript_consequences=snv_indel.sorted_transcript_consequences(ht, gencode_ensembl_to_refseq_id_mapping=gencode_ensembl_to_refseq_id_mapping),
    sorted_regulatory_feature_consequences=snv_indel.sorted_regulatory_feature_consequences(ht),
    sorted_motif_feature_consequences=snv_indel.sorted_motif_feature_consequences(ht),
)
ht = ht.drop('vep', 'vep_proc_id')
ht = misc.annotate_enums(ht, ReferenceGenome.GRCh38, DatsetType.SNV_INDEL)
write(ht, 'gs://seqr-scratch-temp/annotations.ht')