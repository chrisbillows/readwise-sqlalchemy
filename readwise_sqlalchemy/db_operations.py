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

    # class DatabasePopulater:
    #     def __init__(
    #         self,
    #         session: Session,
    #         validated_books: list[BookSchema],
    #         start_fetch: datetime,
    #         end_fetch: datetime,
    #     ):
    #         """Initialiser.

    #         Parameters
    #         ----------
    #         session: Session
    #             An SQL Alchemy session.
    #         validated_books: list[BookSchema]
    #             A list of Books and their highlights, expected to come from the Readwise
    #             Export API.
    #         start_fetch: datetime
    #             The time when the API fetch was started.
    #         end_fetch: datetime
    #             The time when the API fetch was completed.
    #         """
    #         self.session = session
    #         self.validated_books = validated_books
    #         self.start_fetch = start_fetch
    #         self.end_fetch = end_fetch

    #     def populate_database(self) -> None:
    #         """Populate the database with books and highlights from a Readwise API response.

    #         Readwise highlights are exported as books, with each book containing a list
    #         of highlights. If specified, only highlights created since the 'last_fetch' date
    #         are included. i.e. A book might have 100 highlights, but if only 1 highlight has
    #         been added since the last fetch, only 1 highlight will be in the highlights
    #         list.

    #         This method records the ReadwiseBatch and then iterates over each book, and each
    #         list of highlights for each book, validating and adding books and highlights to
    #         the passed session object. This session is then either successfully committed
    #         to the database, or the database is rolled back to it's previous state.
    #         """
    #         batch = ReadwiseBatch(
    #             start_time=self.start_fetch,
    #             end_time=self.end_fetch,
    #             database_write_time=datetime.now(),
    #         )
    #         self.session.add(batch)

    #         for book_as_schema in self.validated_books:
    #             book_as_orm = Book(
    #                 **book_as_schema.model_dump(exclude={"highlights", "book_tags"}),
    #                 batch=batch,
    #             )

    #             for highlight in book_as_schema.highlights:
    #                 highlight_data = highlight.model_dump()
    #                 highlight_tags = [
    #                     HighlightTag(**tag, batch=batch)
    #                     for tag in highlight_data.pop("tags", [])
    #                 ]
    #                 highlight_as_orm = Highlight(**highlight_data, batch=batch)
    #                 highlight_as_orm.tags.extend(highlight_tags)
    #                 book_as_orm.highlights.append(highlight_as_orm)

    #             for book_tag in book_as_schema.book_tags:
    #                 book_tag_data = book_tag.model_dump()
    #                 book_tag_as_orm = BookTag(**book_tag_data, batch=batch)
    #                 book_as_orm.book_tags.append(book_tag_as_orm)

    #             self.session.add(book_as_orm)
    #         try:
    #             logging.info("Committing session")
    #             self.session.commit()
    #         except Exception as e:
    #             self.session.rollback()
    #             logging.info(f"Error occurred: {e}")

    def _process_book(self, book: dict[Any, Any]) -> Book:
        """Process a book.

        Checks if a book exists in the database and, if it does not, adds it to the
        session. Expects that 'highlights' are not present in the passed in dictionary.

        Returns
        -------
        Book
            The book object that either already exists in the database or has been
            added to the session.
        """
        logging.info("Book title:", book["title"])
        existing_book = (
            self.session.query(Book)
            .filter_by(user_book_id=book["user_book_id"])
            .first()
        )
        if not existing_book:
            logging.info("Book not in database")
            new_book = Book(**book)
            self.session.add(new_book)
            self.session.flush()
            logging.info(
                f"Added new book: {book['title']}, ID: {new_book.user_book_id}"
            )
            return new_book
        else:
            logging.info(f"Book with ID {book['user_book_id']} already exists")
            return existing_book

    def _process_highlight(self, highlight: dict[Any, Any], book: Book) -> None:
        """Process a highlight and add it to the session.

        Perform validation checks, field processing and add the highlight to the
        session.
        """
        logging.info(f"Highlight: {highlight['text'][:20]}")

        # highlight["highlighted_at"] = convert_iso_to_datetime(
        #     highlight.get("highlighted_at")
        # )
        # highlight["created_at"] = convert_iso_to_datetime(highlight["created_at"])
        # highlight["updated_at"] = convert_iso_to_datetime(highlight["updated_at"])

        # SQL Alchemy will ignore in favour of 'book' but included to be explicit.
        highlight.pop("book_id", None)

        highlight_obj = Highlight(**highlight, book=book)
        self.session.add(highlight_obj)


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
