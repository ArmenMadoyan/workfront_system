"""Shared core package: models, db session, config, and event/command schemas.

Submodules are imported on demand (e.g. `from wf_core import models`) so that
lightweight consumers don't drag in a DB driver via `db`.
"""

__all__ = ["config", "db", "events", "ids", "models"]