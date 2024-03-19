import luigi

from v03_pipeline.lib.tasks import *  # noqa: F403

if __name__ == '__main__':
    luigi.run(['WriteCachedReferenceDatasetQuery', '--local-scheduler'])
