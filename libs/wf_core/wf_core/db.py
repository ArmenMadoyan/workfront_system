"""SQLAlchemy engine/session factories shared across services.

Two stacks share the same models/URL (psycopg 3 does both sync and async):
  * SYNC  — for the worker, projector, migrations, scripts.
  * ASYNC — for the api gateway (async FastAPI + WebSockets).

Everything is created lazily so importing wf_core never requires a DB driver
or a reachable database (e.g. the pure cascade logic imports nothing here).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker

from wf_core.config import settings


# --- sync (worker / projector / migrations) ----------------------------------
@lru_cache(maxsize=1)
def get_engine() -> Engine:
    return create_engine(settings.database_url, pool_pre_ping=True, future=True)


@lru_cache(maxsize=1)
def _session_factory() -> sessionmaker:
    return sessionmaker(bind=get_engine(), expire_on_commit=False, future=True)


def SessionLocal() -> Session:
    return _session_factory()()


@contextmanager
def session_scope() -> Iterator[Session]:
    """Transactional scope: commit on success, rollback on error."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# --- async (api gateway) ------------------------------------------------------
@lru_cache(maxsize=1)
def get_async_engine() -> AsyncEngine:
    return create_async_engine(settings.database_url, pool_pre_ping=True)


@lru_cache(maxsize=1)
def _async_session_factory() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=get_async_engine(), expire_on_commit=False)


@asynccontextmanager
async def async_session_scope() -> AsyncIterator[AsyncSession]:
    """Async transactional scope: commit on success, rollback on error."""
    session = _async_session_factory()()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()