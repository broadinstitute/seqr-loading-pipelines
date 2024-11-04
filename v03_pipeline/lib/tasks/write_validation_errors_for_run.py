import json

import luigi
import luigi.util

from v03_pipeline.lib.paths import validation_errors_for_run_path
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


@luigi.util.inherits(BaseLoadingRunParams)
class WriteValidationErrorsForRunTask(luigi.Task):
    project_guids = luigi.ListParameter()
    error_messages = luigi.ListParameter(default=[])

    def to_single_error_message(self) -> str:
        with self.output().open('r') as f:
            error_messages = json.load(f)['error_messages']
            if len(error_messages) == 1:
                return error_messages[0]
            return f'Multiple validation errors encountered: {error_messages}'

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            validation_errors_for_run_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def run(self) -> None:
        validation_errors_json = {
            'project_guids': self.project_guids,
            'error_messages': self.error_messages,
        }
        with self.output().open('w') as f:
            json.dump(validation_errors_json, f)
