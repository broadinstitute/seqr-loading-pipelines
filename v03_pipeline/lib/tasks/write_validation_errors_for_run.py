import json
from collections.abc import Callable

import luigi
import luigi.freezing
import luigi.util

from v03_pipeline.lib.misc.validation import SeqrValidationError
from v03_pipeline.lib.paths import validation_errors_for_run_path
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


@luigi.util.inherits(BaseLoadingRunParams)
class WriteValidationErrorsForRunTask(luigi.Task):
    project_guids = luigi.ListParameter()
    error_messages = luigi.ListParameter(default=[])
    failed_family_samples = luigi.DictParameter(default={})

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
            'failed_family_samples': luigi.freezing.recursively_unfreeze(
                self.failed_family_samples,
            ),
        }
        with self.output().open('w') as f:
            json.dump(validation_errors_json, f)


def with_persisted_validation_errors(f: Callable) -> Callable[[Callable], Callable]:
    def wrapper(self: luigi.Task):
        try:
            return f(self)
        except SeqrValidationError as e:
            if isinstance(
                e.args[1],
                object,
            ):  # TODO: improve type checking with a pydantic model/typed dict
                write_validation_errors_for_run_task = self.clone(
                    WriteValidationErrorsForRunTask,
                    error_messages=[str(e.args[0])],
                    failed_family_samples=e.args[1]['failed_family_samples'],
                )
            else:
                write_validation_errors_for_run_task = self.clone(
                    WriteValidationErrorsForRunTask,
                    error_messages=[str(e)],
                )
            write_validation_errors_for_run_task.run()
            raise SeqrValidationError(
                write_validation_errors_for_run_task.to_single_error_message(),
            ) from None

    return wrapper
