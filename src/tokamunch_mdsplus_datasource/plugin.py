from __future__ import annotations

from .datasource import MDSplusDataSource


def create_data_source(args: dict | None = None) -> MDSplusDataSource:
    return MDSplusDataSource(args or {})
