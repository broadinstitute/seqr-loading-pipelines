from enum import StrEnum

MIGRATION_RUN_ID = 'hail_search_to_clickhouse'


class ClickHouseMigrationType(StrEnum):
    PROJECT_ENTRIES = 'PROJECT_ENTRIES'
    VARIANTS = 'VARIANTS'

    @property
    def run_id(self):
        return f'{MIGRATION_RUN_ID}_{self.value}'
