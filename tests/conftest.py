"""
Pytest fixtures and reusable test data, helpers etc.

"""

from dataclasses import dataclass

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from readwise_sqlalchemy.config import UserConfig
from readwise_sqlalchemy.db_operations import safe_create_sqlite_engine
from readwise_sqlalchemy.models import Base


@pytest.fixture
def mock_user_config(tmp_path: pytest.TempPathFactory) -> UserConfig:
    """Return a temporary readwise-sqlalchemy user configuration.

    Use a `tmp_path` as the User's home directory. Create the directory and create the
    required .env file with required synthetic data.
    """
    temp_application_dir = tmp_path / "readwise-sqlalchemy-application"
    temp_application_dir.mkdir()

    temp_env_file = temp_application_dir / ".env"
    temp_env_file.touch()
    temp_env_file.write_text("READWISE_API_TOKEN = 'abc123'")
    return UserConfig(temp_application_dir)


@dataclass
class DbHandle:
    """Group SQL Alchemy database connection objects.

    Attributes
    ----------
    engine: Engine
        Engine bound to a database.
    session: Session
        Session bound to an engine.
    """

    engine: Engine
    session: Session


@pytest.fixture()
def mem_db():
    """
    Create an in-memory SQLite database and return an engine and session.

    Creates tables for all ORM mapped classes that inherit from Base.
    """
    engine = safe_create_sqlite_engine(":memory:", echo=False)
    Base.metadata.create_all(engine)
    # This isn't needed. It's included as a best practice example or for future use.
    SessionMaker = sessionmaker(bind=engine)
    session = SessionMaker()
    yield DbHandle(engine, session)
    session.close()
