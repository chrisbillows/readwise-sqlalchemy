"""
Logic for writing updates to the DB
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
    Highlight,
    HighlightTag,
    ReadwiseBatch,
)
from readwise_sqlalchemy.schemas import BookSchema

logger = logging.getLogger(__name__)


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


# TODO: Do we need this.
class ReadwiseAPIValidator:
    """
    Validate Readwise API responses.

    Pydantic validation layer for the Readwise API 'Highlight EXPORT' endpoint.

    """

    def __init__(self, api_response: list[dict[str, str | int | datetime]]):
        self.api_response = api_response


class DatabasePopulater:
    def __init__(
        self,
        session: Session,
        books: list[dict[Any, Any]],
        start_fetch: datetime,
        end_fetch: datetime,
    ):
        """Initialiser.

        Parameters
        ----------
        session: Session
            An SQL Alchemy session.
        books: list[dict[Any, Any]]
            A list of Books and their highlights, expected to come from the Readwise
            Export API.
        start_fetch: datetime
            The time when the API fetch was started.
        end_fetch: datetime
            The time when the API fetch was completed.
        """
        self.session = session
        self.books = books
        self.start_fetch = start_fetch
        self.end_fetch = end_fetch

    def populate_database(self) -> None:
        """Populate the database with books and highlights from a Readwise API response.

        Readwise highlights are exported as books, with each book containing a list
        of highlights. If specified, only highlights created since the 'last_fetch' date
        are included. i.e. A book might have 100 highlights, but if only 1 highlight has
        been added since the last fetch, only 1 highlight will be in the highlights
        list.

        This method records the ReadwiseBatch and then iterates over each book, and each
        list of highlights for each book, validating and adding books and highlights to
        the passed session object. This session is then either successfully committed
        to the database, or the database is rolled back to it's previous state.
        """
        batch = ReadwiseBatch(
            start_time=self.start_fetch,
            end_time=self.end_fetch,
            database_write_time=datetime.now(),
        )
        self.session.add(batch)

        for book in self.books:
            book_as_schema = BookSchema(**book)
            book_as_orm = Book(
                **book_as_schema.model_dump(exclude={"highlights"}), batch=batch
            )
            for highlight in book_as_schema.highlights:
                highlight_data = highlight.model_dump()
                highlight_tags = [
                    HighlightTag(**tag, batch=batch)
                    for tag in highlight_data.pop("tags", [])
                ]
                highlight_as_orm = Highlight(**highlight_data, batch=batch)
                highlight_as_orm.tags.extend(highlight_tags)
                # highlight_as_orm.book = book_as_orm
                book_as_orm.highlights.append(highlight_as_orm)
            self.session.add(book_as_orm)
        try:
            logging.info("Committing session")
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logging.info(f"Error occurred: {e}")

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

    def _validate_book_id(self, highlight: dict[Any, Any], book: Book) -> None:
        """Validate a highlight's 'book_id'.

        Check the highlight 'book_id' matches the book 'user_book_id'. A previous step
        is presumed to have confirmed the book appears in the database or session.

        Raises
        ------
        ValueError
            If the 'book_id' and 'user_book_id' don't match.
        KeyError
            If 'book_id' not present in a highlight; all highlights should this key.
        """
        if highlight["book_id"] != book.user_book_id:
            raise ValueError(
                f"Mismatch in book IDs: Highlight 'book_id'={highlight['book_id']} "
                f"does not match Book 'user_book_id'={book.user_book_id}"
            )

    def _validate_highlight_id(self, highlight: dict[Any, Any], book: Book) -> None:
        """Validate a highlights 'id'.

        Check if the highlight 'id' already exists in the database.

        Note
        ----
        Book is passed to create a more helpful error message.

        Raises
        ------
        ValueError
            If the highlight 'id' already exists in the database. This should not be
            possible since API fetches should only contain new highlights.
        """
        existing_highlight = (
            self.session.query(Highlight).filter_by(id=highlight["id"]).first()
        )
        if existing_highlight:
            raise ValueError(
                f"Highlight ID already in database: Highlight 'id'={highlight['id']},  "
                f"Book 'user_book_id'={book.user_book_id}"
            )

    def _process_highlight(self, highlight: dict[Any, Any], book: Book) -> None:
        """Process a highlight and add it to the session.

        Perform validation checks, field processing and add the highlight to the
        session.
        """
        logging.info(f"Highlight: {highlight['text'][:20]}")
        self._validate_book_id(highlight, book)
        self._validate_highlight_id(highlight, book)

        # highlight["highlighted_at"] = convert_iso_to_datetime(
        #     highlight.get("highlighted_at")
        # )
        # highlight["created_at"] = convert_iso_to_datetime(highlight["created_at"])
        # highlight["updated_at"] = convert_iso_to_datetime(highlight["updated_at"])

        # SQL Alchemy will ignore in favour of 'book' but included to be explicit.
        highlight.pop("book_id", None)

        highlight_obj = Highlight(**highlight, book=book)
        self.session.add(highlight_obj)


def query_get_last_fetch(session: Session) -> datetime | None:
    """Get the last fetch."""
    stmt = (
        select(ReadwiseBatch).order_by(desc(ReadwiseBatch.database_write_time)).limit(1)
    )
    result = session.execute(stmt).scalars().first()
    if result:
        return result.database_write_time
    else:
        return None
