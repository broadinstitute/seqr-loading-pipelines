from abc import ABC, abstractmethod

import hail as hl

from hail_scripts.utils import hail_utils


class HailVEPRunnerBase(ABC):

    @abstractmethod
    def run(self, mt, genome_version, vep_config_json_path=None):
        pass


class HailVEPRunner(HailVEPRunnerBase):

    def run(self, mt, genome_version, vep_config_json_path=None):
        return hail_utils.run_vep(mt, genome_version, vep_config_json_path=vep_config_json_path)


class HailVEPDummyRunner(HailVEPRunnerBase):
    """ Dummy hail runner used in environments (e.g. local) when a VEP installation is not available to run.

    Mocked data all from rsid `rs35471880`. Ideally this should be generic data and marked as mocked, but
    that would require a lot of domain knowledge.

    """


    def run(self, mt, genome_version, vep_config_json_path=None):
        if isinstance(mt, hl.Table):
            return mt.annotate(vep=self.MOCK_VEP_DATA)
        return mt.annotate_rows(vep=self.MOCK_VEP_DATA)
