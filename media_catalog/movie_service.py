"""Movie HTTP service entry point."""

from media_catalog.source_service import create_source_app

def create_app(database_path: str | None = None):
    return create_source_app("movie", database_path)


app = create_app()
