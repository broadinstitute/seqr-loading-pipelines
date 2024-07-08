import glob
import re

MIGRATIONS_PATH = 'v03_pipeline/migrations/*.py'
MIGRATION_NAME_PATTERN = r'(\d\d\d\d_.*)\.py'


def list_migrations(
    path: str = MIGRATIONS_PATH,
    name_pattern: str = MIGRATION_NAME_PATTERN,
) -> list[str]:
    migrations = []
    for migration in glob.glob(path):
        match = re.search(name_pattern, migration)
        if match:
            migrations.append(match.group(1))
    return migrations
