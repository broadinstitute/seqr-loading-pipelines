import inspect
import pkgutil
import re

from v03_pipeline.lib.migration.base_migration import BaseMigration

MIGRATION_NAME_PATTERN = r'(\d\d\d\d_.*)'


def list_migrations(
    path: str,
) -> list[tuple[str, BaseMigration]]:
    migrations = []
    for loader, name, _ in pkgutil.iter_modules([path]):
        match = re.search(MIGRATION_NAME_PATTERN, name)
        if match:
            module = loader.find_module(name).load_module(name)
            implemented_migration = next(
                (
                    m
                    for m in module.__dict__.values()
                    # Return objects that are
                    # classes, subclasses of the BaseMigration
                    # and also NOT the BaseMigration class.
                    if inspect.isclass(m)
                    and issubclass(m, BaseMigration)
                    and m != BaseMigration
                ),
                None,
            )
            if implemented_migration:
                migrations.append((match.group(1), implemented_migration))
    return sorted(migrations, key=lambda x: x[0])
