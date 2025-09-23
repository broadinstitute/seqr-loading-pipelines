import json
from collections import defaultdict

import hail as hl
import hailtop.fs as hfs
import luigi
import luigi.util
import onnx

from v03_pipeline.lib.methods.sample_qc import call_sample_qc
from v03_pipeline.lib.misc.io import import_tdr_qc_metrics
from v03_pipeline.lib.paths import (
    ancestry_model_rf_path,
    sample_qc_json_path,
    tdr_metrics_dir,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.files import GCSorLocalTarget, RawFileTask
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset import (
    UpdatedReferenceDatasetTask,
)
from v03_pipeline.lib.tasks.validate_callset import ValidateCallsetTask
from v03_pipeline.lib.tasks.write_tdr_metrics_files import WriteTDRMetricsFilesTask


@luigi.util.inherits(BaseLoadingRunParams)
class WriteSampleQCJsonTask(luigi.Task):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            sample_qc_json_path(
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
            ),
        )

    def requires(self):
        return [
            self.clone(ValidateCallsetTask),
            self.clone(WriteTDRMetricsFilesTask),
            self.clone(
                UpdatedReferenceDatasetTask,
                reference_dataset=ReferenceDataset.gnomad_qc,
            ),
            RawFileTask(ancestry_model_rf_path()),
        ]

    def run(self):
        callset_mt = hl.read_matrix_table(self.input()[0].path)
        tdr_metrics_ht = None
        for tdr_metrics_file in hfs.ls(
            tdr_metrics_dir(self.reference_genome, self.dataset_type),
        ):
            if not tdr_metrics_ht:
                tdr_metrics_ht = import_tdr_qc_metrics(tdr_metrics_file.path)
                continue
            tdr_metrics_ht = tdr_metrics_ht.union(
                import_tdr_qc_metrics(tdr_metrics_file.path),
            )
        pop_pca_loadings_ht = hl.read_table(self.input()[2].path)
        with hl.hadoop_open(self.input()[3].path, 'rb') as f:
            ancestry_rf_model = onnx.load(f)
        callset_mt = call_sample_qc(
            callset_mt,
            tdr_metrics_ht,
            pop_pca_loadings_ht,
            ancestry_rf_model,
            self.sample_type,
        )
        ht = callset_mt.cols()
        sample_qc_dict = defaultdict(dict)
        for row in ht.flatten().collect():
            r = dict(row)
            sample_id = r.pop('s')
            for field, value in r.items():
                sample_qc_dict[sample_id][field] = value

        with self.output().open('w') as f:
            json.dump(sample_qc_dict, f)
