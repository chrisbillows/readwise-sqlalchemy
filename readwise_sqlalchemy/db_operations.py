"""
Logic for interacting with the database.
"""

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Union

from sqlalchemy import Engine, create_engine, desc, event, select
from sqlalchemy.orm import Session, sessionmaker

from readwise_sqlalchemy.models import (
    Base,
    Book,
    BookTag,
    Highlight,
    HighlightTag,
    ReadwiseBatch,
)

logger = logging.getLogger(__name__)

MODELS_BY_OBJECT = {
    "books": Book,
    "book_tags": BookTag,
    "highlights": Highlight,
    "highlight_tags": HighlightTag,
}


def safe_create_sqlite_engine(
    sqlite_database: Union[str, Path], echo: bool = False
) -> Engine:
    """
    Create a SQLite engine with foreign key enforcement enabled for all connections.

    Foreign key constraints are not enforced by default in SQLite. They must be
    enabled for every new database connection using the ``PRAGMA foreign_keys=ON``
    statement, as PRAGMA settings do not persist in the database file. This function
    creates a SQLAlchemy ``Engine`` configured with an event listener that ensures
    this PRAGMA statement is always executed.

    Create SQLite engines with this function to ensure consistent foreign key behaviour.
    For more details, see:
    https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#foreign-key-support

    Parameters
    ----------
    sqlite_database : Union[str, Path]
        The filename of the SQLite database file, or ':memory:' for an in-memory
        database.
    echo : bool, optional
        If ``True``, SQLAlchemy will log all SQL statements (default is ``False``).

    Returns
    -------
    Engine
        A SQLAlchemy ``Engine`` instance that will enforce foreign key constraints
        on all connections.
    """

    def set_sqlite_pragma(
        dbapi_connection: sqlite3.Connection, connection_record: Any
    ) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    db_path = "sqlite:///" + str(sqlite_database)
    engine = create_engine(db_path, echo=echo)
    event.listen(engine, "connect", set_sqlite_pragma)
    return engine


def get_session(database_path: str | Path) -> Session:
    """
    Establish a connection to the database and return a session.

    Parameters
    ----------
    database_path : str
    """
    engine = safe_create_sqlite_engine(database_path)
    SessionFactory = sessionmaker(bind=engine)
    session = SessionFactory()
    return session


def create_database(database_path: str | Path) -> None:
    """Create the database schema. This should only be called during setup."""
    engine = safe_create_sqlite_engine(database_path)
    Base.metadata.create_all(engine)


class DatabasePopulaterFlattenedData:
    def __init__(
        self,
        session: Session,
        validated_flattened_objs: dict[str, list[dict[str, Any]]],
        start_fetch: datetime,
        end_fetch: datetime,
    ):
        """Initialiser.

        Parameters
        ----------
        session: Session
            An SQL Alchemy session.
        validated_flattened_objs: dict[str, list[dict[str, Any]]]
            The flattened API data with each object having additional validation fields.
            The dict keys are the object type (books, book_tags etc) and values for each
            are a list of that type of object.
        start_fetch: datetime
            The time when the API fetch was started.
        end_fetch: datetime
            The time when the API fetch was completed.
        """
        self.session = session
        self.validated_flattened_objs = validated_flattened_objs
        self.start_fetch = start_fetch
        self.end_fetch = end_fetch

    def populate_database(self) -> None:
        """
        Populate the database with books and highlights from a Readwise API response.

        Readwise highlights are exported as books, with each book containing a list
        of highlights. If specified, only highlights created since the 'last_fetch' date
        are included. i.e. A book might have 100 highlights, but if only 1 highlight has
        been added since the last fetch, only 1 highlight will be in the highlights
        list.

        This method records the ReadwiseBatch and then iterates over the unnested and
        validated books, book tags, highlights and highlight tags, adding the objects
        to the session. This session is then either successfully committed
        to the database, or the database is rolled back to it's previous state.
        """
        batch = ReadwiseBatch(
            start_time=self.start_fetch,
            end_time=self.end_fetch,
            database_write_time=datetime.now(),
        )
        self.session.add(batch)
        for obj_name, objects in self.validated_flattened_objs.items():
            obj_orm = MODELS_BY_OBJECT[obj_name]
            for object in objects:
                obj_as_orm = obj_orm(**object, batch=batch)
                self.session.add(obj_as_orm)
        try:
            logging.info("Committing session")
            self.session.commit()
        except Exception as err:
            self.session.rollback()
            logging.info(f"Error occurred committing session: {err}")
            raise err


def get_last_fetch(session: Session) -> datetime | None:
    """
    Get the time of the last Readwise API fetch from the database.

    The 'last fetch' uses the *start* time of the previous fetch, to allow for an
    overlap. Validation removes duplicated book ids/highlights.

    Parameters
    ----------
    session: Session
        A SQL alchemy session connected to a database.

    Returns
    -------
    datetime | None
        A datetime object representing the start time of the last fetch, or None.
    """
    stmt = select(ReadwiseBatch).order_by(desc(ReadwiseBatch.start_time)).limit(1)
    result = session.execute(stmt).scalars().first()
    if result:
        return result.database_write_time
    else:
        return None
