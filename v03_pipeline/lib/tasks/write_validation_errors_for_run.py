import json

import luigi
import luigi.util

from v03_pipeline.lib.paths import validation_errors_for_run_path
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


@luigi.util.inherits(BaseLoadingRunParams)
class WriteValidationErrorsForRunTask(luigi.Task):
    error_messages = luigi.ListParameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            validation_errors_for_run_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def complete(self) -> bool:
        return GCSorLocalTarget(self.output().path).exists()

    def run(self) -> None:
        validation_errors_json = {
            'error_messages': [error_message for e in error_messages],
        }
        with self.output().open('w') as f:
            json.dump(validation_errors_json, f)
