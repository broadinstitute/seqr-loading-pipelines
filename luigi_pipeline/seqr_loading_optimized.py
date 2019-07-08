import logging
import os
import re

import luigi
import hail as hl

from hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient
from lib.hail_tasks import HailMatrixTableTask, HailElasticSearchTask, GCSorLocalTarget, MatrixTableSampleSetError
from lib.model.seqr_mt_schema import SeqrSchema, SeqrVariantSchema, SeqrGenotypesSchema
import seqr_loading
# from gnomad_hail.utils import vep_or_lookup_vep

logger = logging.getLogger(__name__)


class SeqrValidationError(Exception):
    pass

class SeqrVCFToVariantHTTask(HailMatrixTableTask):
    """
    Inherits from a Hail MT Class to get helper function logic. Main logic to do annotations here.
    """
    variant_dir = luigi.Parameter(default='variant_hts')
    reference_ht_path = luigi.Parameter(description='Path to the Hail table storing the reference variants.')
    clinvar_ht_path = luigi.Parameter(description='Path to the Hail table storing the clinvar variants.')
    hgmd_ht_path = luigi.Parameter(description='Path to the Hail table storing the hgmd variants.')

    def output(self):
        return GCSorLocalTarget(os.path.join(self.variant_dir, 'full.ht'))
        max_version, full_path = self.latest_variant_ht()

        if full_path:
            cur_variant_ht = hl.read_table(full_path)
            mt = hl.split_multi_hts(self.import_vcf())
            if cur_variant_ht.count() == mt.rows().count():
                return GCSorLocalTarget(full_path)

        return GCSorLocalTarget(os.path.join(self.variant_dir, 'variants-v%s.ht' % (max_version+1)))


    def run(self):
        mt = hl.import_vcf(self.source_paths[0], force_bgz=True)
        mt = hl.split_multi_hts(mt)
        # Same as above, made sure mega_variant_ht is None.
        mega_variant_ht = None # hl.read_table(self.latest_variant_ht()[1]) if self.latest_variant_ht()[1] else None
        if mega_variant_ht:
            mt = mt.anti_join_rows(mega_variant_ht)
        mt = HailMatrixTableTask.run_vep(mt, self.genome_version, self.vep_runner)
        # We're now adding ref data.
        ref_data = hl.read_table(self.reference_ht_path)
        clinvar = hl.read_table(self.clinvar_ht_path)
        hgmd = hl.read_table(self.hgmd_ht_path)

        new_ht = SeqrVariantSchema(mt, ref_data=ref_data, clinvar_data=clinvar, hgmd_data=hgmd).annotate_all(
            overwrite=True).select_annotated_mt().rows()

        mt.describe()

        if mega_variant_ht:
            new_ht = mega_variant_ht.union(new_ht)

        new_ht.write(self.output().path, stage_locally=True)
        hl.copy_log(self.output().path + '-logs.log')

    def latest_variant_ht(self):
        variant_dir = self.variant_dir
        fs = luigi.contrib.gcs.GCSClient() if variant_dir.startswith('gs:') else luigi.local_target.LocalFileSystem()

        max_version = 0
        latest_full_path = None
        ht_name_regex = re.compile('(.*variants-v([0-9]*)\.ht.*)/_SUCCESS$')
        for path in fs.listdir(variant_dir):
            match = ht_name_regex.match(path)
            if not match:
                continue
            version = int(match.group(2))
            if version > max_version:
                max_version = version
                latest_full_path = os.path.join(variant_dir, 'variants-v%s.ht' % max_version)

        return max_version, latest_full_path


class SeqrVCFToMTTask2(seqr_loading.SeqrVCFToMTTask):

    def run(self):
        mt = hl.import_vcf('gs://seqr-datasets/GRCh37/RDG_WES_Broad_Internal/v10/RDG_WES_Broad_Internal.vcf.gz', force_bgz=True)
        mt = hl.split_multi_hts(mt)
        if self.remap_path:
            mt = self.remap_sample_ids(mt, self.remap_path)
        if self.subset_path:
            mt = self.subset_samples_and_variants(mt, self.subset_path)

        mt = SeqrGenotypesSchema(mt).annotate_all(overwrite=True).select_annotated_mt()

        mt.describe()
        mt.write(self.output().path)


class SeqrMTToESOptiimzedTask(HailElasticSearchTask):
    dest_file = luigi.Parameter(default='_SUCCESS')

    def __init__(self, *args, **kwargs):
        # TODO: instead of hardcoded index, generate from project_guid, etc.
        super().__init__(es_index='data', *args, **kwargs)

    def requires(self):
        return [SeqrVCFToVariantHTTask(), SeqrVCFToMTTask2()]

    def output(self):
        # TODO: Use https://luigi.readthedocs.io/en/stable/api/luigi.contrib.esindex.html.
        return GCSorLocalTarget(filename=self.dest_file)

    def run(self):
        genotypes_mt = hl.read_matrix_table(self.input()[1].path)
        variants_ht = hl.read_table(self.input()[0].path)
        row_ht = genotypes_mt.rows().join(variants_ht)

        row_ht = row_ht.drop('vep').flatten()
        row_ht = row_ht.drop(row_ht.locus, row_ht.alleles).annotate(type='optimized')
        self.export_table_to_elasticsearch(row_ht)

        self.cleanup()


if __name__ == '__main__':
    luigi.run()