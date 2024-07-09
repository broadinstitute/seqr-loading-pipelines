import inspect
import pkgutil
import re

import v03_pipeline.migrations
from v03_pipeline.migrations.base_migration import BaseMigration

MIGRATION_NAME_PATTERN = r'(\d\d\d\d_.*)'


# NB: there's a strong case for this method to just be included
# in the migrations package... but it was easier to
# unit test by mocking the entire package itself.
def list_migrations(
    path: str = v03_pipeline.migrations.__path__,
) -> list[tuple[str, BaseMigration]]:
    migrations = []
    for loader, name, _ in pkgutil.iter_modules([path]):
        match = re.search(MIGRATION_NAME_PATTERN, name)
        if match:
            module = loader.find_module(name).load_module(name)
            implemented_migration = next(
                (
                    x
                    for x in module.__dict__.values()
                    # Return objects that are
                    # classes, subclasses of the BaseMigration
                    # and also NOT the BaseMigration class.
                    if inspect.isclass(x)
                    and issubclass(x, BaseMigration)
                    and x != BaseMigration
                ),
                None,
            )
            if implemented_migration:
                migrations.append((match.group(1), implemented_migration))
    return sorted(migrations, key=lambda x: x[0])
