"""
SQLAlchemy mapped classes and associated types and validators.

Readwise primary keys as used as database primary keys throughout for consistency with
Readwise object relationships.

Pydantic validation is assumed, including ensuring all fields are present. Database
column level validation, where used, is not for validation but additional security and
performance.

Note
----
Some validation may seem to occur, but doesn't:

- Type hints e.g. ``Mapped[int]`` and character limits e.g. ``String(511)`` are not
  enforced at runtime by SQLAlchemy. The underlying database dialect may - or may not -
  enforce them. SQLite is particularly permissive and enforces neither datatype nor
  character limits.

- Missing fields are accepted and default to None. This only results in an error when
  committing to the database and only if the field is ``nullable=False``.

"""

import sqlite3
from datetime import datetime
from typing import Any, List

from sqlalchemy import (
    Dialect,
    Engine,
    ForeignKey,
    String,
    create_engine,
    event,
    inspect,
    select,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
)
from sqlalchemy.types import TypeDecorator


class CommaSeparatedList(TypeDecorator):
    """
    Convert to/from a list of strings/a comma separated list as a single string.

    A custom SQLAlchemy type extending the SQL String type via TypeDecorator to store a
    list of strings as a comma separated list, rendered as a single str. (SQLite has no
    list types). The custom behaviour is provided by overriding the TypeDecorator
    methods ``process_bind_param`` and  ``process_result_value``.

                list[str]                                   str^
                    |                                        |
                    |                                        |
                write to db                             read from db
                    |                                        |
                    |                                        |
                    v                                        V
                   str^                                   list[str]

    ^The str is a comma separated list rendered as a single string.

    See:
    https://docs.sqlalchemy.org/en/20/core/custom_types.html#augmenting-existing-types
    """

    impl = String
    # Safe to cache as the same values will always give the same results.
    cache_ok = True

    def process_bind_param(self, value: list[str], dialect: Dialect) -> str:
        """
        Receive a bound parameter of type ``list[str]`` and convert to ``str``.

        Override the TypeDecorator method. The method is called at statement execution
        time and is passed the literal Python data value which is to be associated with
        a bound parameter in the statement.

        Parameters
        ----------
        value: list[str]
            Data to operate upon, here a list of strings.

        dialect: Dialect
            The SQL Dialect in use.

        Returns
        -------
        str
            A single Python string representing a list of strings as comma separated
            values.
        """
        return "" if not value else ",".join(value)

    def process_result_value(self, value: str, dialect: Dialect) -> list[str]:
        """
        Receive a result-row column value of type ``str` and convert to ``list[str]``.

        This method is called at result fetching time and is passed the literal Python
        data value extracted from a database result row.

        Parameters
        ----------
        value: str
            Data to operate upon, here a string representing a list of strings as comma
            separated values.

        dialect: Dialect
            The SQL Dialect in use.

        Returns
        -------
        list[str]
            A list of strings.
        """
        return [] if value == "" else value.split(",")


class Base(DeclarativeBase):
    """
    Subclass SQLAlchemy ``DeclarativeBase`` base class.

    All ORM Mapped classes should inherit from ``Base``. Tables can then be created
    with ``Base.metadata.create_all``.
    """

    pass


class Book(Base):
    """
    Readwise book as a SQL Alchemy ORM Mapped class.

    Each instance corresponds to a book dictionary from the Readwise 'Highlight EXPORT'
    endpoint. "books" are the parent objects for all highlights, even those not sourced
    from books. Examples:

    +------------+--------------------------------------------------------------------+
    | Source     | Parent Object                                                      |
    +============+====================================================================+
    | book       | Book                                                               |
    +------------+--------------------------------------------------------------------+
    | x.post     | "Tweets from @<user>". Saved posts are highlights in that entry.   |
    +------------+--------------------------------------------------------------------+
    | x.thread   | Threads are parent objects. Titles are text from the first post.   |
    +------------+--------------------------------------------------------------------+
    | podcast    | The podcast episode.                                               |
    +------------+--------------------------------------------------------------------+
    | article    | The article.                                                       |
    +------------+--------------------------------------------------------------------+
    | youtube    | Individual videos are treated as an article.                       |
    +------------+--------------------------------------------------------------------+

    Attributes
    ----------
    user_book_id : int
        Primary key. Unique identifier sourced from Readwise.
    title: str
        The book's title.
    author: str
        The book's author.

    batch_id:
        Foreign key linking the `id` of the associated `ReadwiseBatch`.

    highlights: list[Highlight]
        A list of highlights sourced from the book.
    batch: ReadwiseBatch
        The batch object the book was imported in.
    """

    __tablename__ = "books"

    user_book_id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(511), nullable=False)
    author: Mapped[str] = mapped_column(nullable=True)

    batch_id: Mapped[int] = mapped_column(
        ForeignKey("readwise_batches.id"), nullable=False
    )

    highlights: Mapped[List["Highlight"]] = relationship(back_populates="book")
    batch: Mapped["ReadwiseBatch"] = relationship(back_populates="books")

    def __repr__(self) -> str:
        return (
            f"Book(user_book_id={self.user_book_id!r}, title={self.title!r}, "
            f"highlights={len(self.highlights)})"
        )


class Highlight(Base):
    """
    Readwise highlight as a SQL Alchemy ORM Mapped class.

    Each instance corresponds to a highlight dictionary from the Readwise 'Highlight
    EXPORT' endpoint. Highlights are text excerpts saved by the user from books,
    articles, or other sources.

    Attributes
    ----------
    id : int
        Primary key. Unique identifier sourced from Readwise.
    text: str
        The actual highlighted text content.

    user_book_id : int
        Foreign key linking the `user_book_id` of the associated `Book`.
    batch_id : int
        Foreign key linking the `id` of the associated `ReadwiseBatch`.

    book : Book
        The book that this highlight belongs to.
    tags : list[HighlightTag]
        Tags assigned to this highlight.
    batch : ReadwiseBatch
        The batch object the highlight was imported in.
    """

    __tablename__ = "highlights"

    id: Mapped[int] = mapped_column(primary_key=True)
    text: Mapped[str] = mapped_column(String(8191), nullable=False)

    user_book_id: Mapped[int] = mapped_column(
        ForeignKey("books.user_book_id"), nullable=False
    )
    batch_id: Mapped[int] = mapped_column(
        ForeignKey("readwise_batches.id"), nullable=False
    )

    book: Mapped["Book"] = relationship(back_populates="highlights")
    tags: Mapped[list["HighlightTag"]] = relationship(back_populates="highlight")
    batch: Mapped["ReadwiseBatch"] = relationship(back_populates="highlights")

    def __repr__(self) -> str:
        return (
            f"Highlight(id={self.id!r}, book_id={self.user_book_id!r}, "
            f"text={self.text!r}, book={self.book.user_book_id!r})"
        )


class HighlightTag(Base):
    """
    Readwise highlight tag as a SQL Alchemy ORM Mapped class.

    Each instance corresponds to a highlight tags dictionary from the Readwise
    'Highlight EXPORT' endpoint.

    Attributes
    ----------
    id : int
        Primary key. Unique identifier sourced from Readwise.
    name : str
        The name of the tag. Each tag has an id and name. ``name``s are often common
        across tags/highlights. ``id`` is always unique. E.g. Many highlights may be
        tagged ``favourite`` but each ``favourite`` tag  will be associated with its own
        unique ``id``. Group by ``name`` for this attribute.

    highlight_id : int
        Foreign key linking the ``id`` of the associated ``Highlight``.
    batch_id : int
        Foreign key linking the `id` of the associated `ReadwiseBatch`.

    highlight: Highlight
        The highlight object the tag is associated with.
    batch: ReadwiseBatch
        The batch object the tag was imported in.
    """

    __tablename__ = "highlight_tags"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]

    highlight_id: Mapped[int] = mapped_column(
        ForeignKey("highlights.id"), nullable=False
    )
    batch_id: Mapped[int] = mapped_column(
        ForeignKey("readwise_batches.id"), nullable=False
    )

    highlight: Mapped["Highlight"] = relationship(back_populates="tags")
    batch: Mapped["ReadwiseBatch"] = relationship(back_populates="highlight_tags")

    def __repr__(self) -> str:
        return (
            f"HighlightTag(id={self.id!r}, name={self.name!r}, "
            f"h_id={self.highlight_id!r})"
        )


class ReadwiseBatch(Base):
    """
    A batch of database updates from the Readwise API.

    Attributes
    ----------
    id : int
        Primary key. Auto generated unique identifier for the batch .
    start_time : datetime
        The start time of a fetch from the API.
    end_time : datetime
        The time the fetch completed.
    database_write_time : Optional[datetime]
        The time the batch was written to the database. Can be None if unset but this is
        intended only to allow this attribute to be added last.

    books : list[Book]
        The books included in the batch.
    highlights : list[Highlight]
        The highlights included in the batch.
    highlight_tags : list[HighlightTag]
        The highlight tags included in the batch.
    """

    __tablename__ = "readwise_batches"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    start_time: Mapped[datetime] = mapped_column(nullable=False)
    end_time: Mapped[datetime] = mapped_column(nullable=False)
    database_write_time: Mapped[datetime] = mapped_column(nullable=True)

    books: Mapped[list["Book"]] = relationship(back_populates="batch")
    highlights: Mapped[list["Highlight"]] = relationship(back_populates="batch")
    highlight_tags: Mapped[list["HighlightTag"]] = relationship(back_populates="batch")

    def __repr__(self) -> str:
        return (
            f"ReadwiseBatch(id={self.id!r}, start={self.start_time!r}, "
            f"end={self.end_time!r}, write={self.database_write_time!r})"
        )


def safe_create_sqlite_engine(sqlite_database: str, echo: bool = False) -> Engine:
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
    sqlite_database : str
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

    db_path = "sqlite:///" + sqlite_database
    engine = create_engine(db_path, echo=echo)
    event.listen(engine, "connect", set_sqlite_pragma)
    return engine


# def convert_iso_to_datetime(date_str: Any | None) -> datetime | None:
#     """Convert an ISO 8601 string to a datetime object."""
#     return datetime.fromisoformat(date_str.replace("Z", "+00:00")) if date_str else None


# class Base(DeclarativeBase):
#     __allow_unmapped__ = True


# Cut from book in a bad decision

# readable_title: Mapped[str] = mapped_column(nullable=False)
# source: Mapped[str] = mapped_column(nullable=True)
# cover_image_url: Mapped[str] = mapped_column(nullable=True)
# unique_url: Mapped[str] = mapped_column(nullable=True)
# summary: Mapped[str] = mapped_column(nullable=True)
# category: Mapped[str] = mapped_column(nullable=True)
# document_note: Mapped[str] = mapped_column(nullable=True)
# readwise_url: Mapped[str] = mapped_column(nullable=True)
# source_url: Mapped[str] = mapped_column(nullable=True)
# book_tags: Mapped[list[str]] = mapped_column(CommaSeparatedList, nullable=False)
# asin: Mapped[Optional[str]] = mapped_column(nullable=True)
# book_tags: Mapped[Optional[List[str]]] = mapped_column(
# CommaSeparatedList, nullable=True )


# class Highlight(Base):
#     __tablename__ = "highlights"

#     id: Mapped[int] = mapped_column(primary_key=True)
#     book_id: Mapped[int] = mapped_column(
#         ForeignKey("books.user_book_id"), nullable=False
#     )
#     text: Mapped[str] = mapped_column(nullable=False)
#     location: Mapped[Optional[int]] = mapped_column(nullable=True)
#     location_type: Mapped[Optional[str]] = mapped_column(nullable=True)
#     note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
#     color: Mapped[Optional[str]] = mapped_column(nullable=True)
#     highlighted_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
#     created_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
#     updated_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
#     external_id: Mapped[Optional[str]] = mapped_column(nullable=True)
#     end_location: Mapped[Optional[int]] = mapped_column(nullable=True)
#     url: Mapped[Optional[str]] = mapped_column(nullable=True)
#     is_favorite: Mapped[bool] = mapped_column(default=False)
#     is_discard: Mapped[bool] = mapped_column(default=False)
#     readwise_url: Mapped[Optional[str]] = mapped_column(nullable=True)
#     tags: Mapped[Optional[List[dict[Any, Any]]]] = mapped_column(
#         JSONEncodedList, nullable=True
#     )

#     book: Mapped["Book"] = relationship(back_populates="highlights")


# class ReadwiseBatch(Base):
#     __tablename__ = "readwise_batches"

#     batch_id: Mapped[int] = mapped_column(primary_key=True)
#     start_time: Mapped[datetime] = mapped_column(nullable=False)
#     end_time: Mapped[datetime] = mapped_column(nullable=False)
#     database_write_time: Mapped[datetime] = mapped_column(nullable=False)


# def create_database(database_path: str | Path) -> None:
#     """Create the database schema. This should only be called during setup."""
#     engine = create_engine(f"sqlite:///{database_path}")
#     Base.metadata.create_all(engine)


# def get_session(database_path: str | Path) -> Session:
#     """Establish a connection to the database and return a session."""
#     engine = create_engine(f"sqlite:///{database_path}")
#     SessionFactory = sessionmaker(bind=engine)
#     session = SessionFactory()
#     return session


# class DatabasePopulater:
#     def __init__(
#         self,
#         session: Session,
#         books: list[dict[Any, Any]],
#         start_fetch: datetime,
#         end_fetch: datetime,
#     ):
#         """Initialiser.

#         Parameters
#         ----------
#         session: Session
#             An SQL Alchemy session.
#         books: list[dict[Any, Any]]
#             A list of Books and their highlights, expected to come from the Readwise
#             Export API.
#         start_fetch: datetime
#             The time when the API fetch was started.
#         end_fetch: datetime
#             The time when the API fetch was completed.
#         """
#         self.session = session
#         self.books = books
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
#         self._process_batch()
#         print("Populating database")

#         for book in self.books:
#             highlights_data = book.pop("highlights", [])
#             book_obj = self._process_book(book)
#             for highlight in highlights_data:
#                 self._process_highlight(highlight, book_obj)

#         try:
#             print("Committing session")
#             self.session.commit()
#         except Exception as e:
#             self.session.rollback()
#             print(f"Error occurred: {e}")

#     def _process_batch(self) -> None:
#         """Add the ReadwiseBatch to the session."""
#         new_batch = ReadwiseBatch(
#             start_time=self.start_fetch,
#             end_time=self.end_fetch,
#             database_write_time=datetime.now(),
#         )
#         self.session.add(new_batch)

#     def _process_book(self, book: dict[Any, Any]) -> Book:
#         """Process a book.

#         Checks if a book exists in the database and, if it does not, adds it to the
#         session. Expects that 'highlights' are not present in the passed in dictionary.

#         Returns
#         -------
#         Book
#             The book object that either already exists in the database or has been
#             added to the session.
#         """
#         print("Book title:", book["title"])
#         existing_book = (
#             self.session.query(Book)
#             .filter_by(user_book_id=book["user_book_id"])
#             .first()
#         )
#         if not existing_book:
#             print("Book not in database")
#             new_book = Book(**book)
#             self.session.add(new_book)
#             self.session.flush()
#             print(f"Added new book: {book['title']}, ID: {new_book.user_book_id}")
#             return new_book
#         else:
#             print(f"Book with ID {book['user_book_id']} already exists")
#             return existing_book

#     def _validate_book_id(self, highlight: dict[Any, Any], book: Book) -> None:
#         """Validate a highlight's 'book_id'.

#         Check the highlight 'book_id' matches the book 'user_book_id'. A previous step
#         is presumed to have confirmed the book appears in the database or session.

#         Raises
#         ------
#         ValueError
#             If the 'book_id' and 'user_book_id' don't match.
#         KeyError
#             If 'book_id' not present in a highlight; all highlights should this key.
#         """
#         if highlight["book_id"] != book.user_book_id:
#             raise ValueError(
#                 f"Mismatch in book IDs: Highlight 'book_id'={highlight['book_id']} "
#                 f"does not match Book 'user_book_id'={book.user_book_id}"
#             )

#     def _validate_highlight_id(self, highlight: dict[Any, Any], book: Book) -> None:
#         """Validate a highlights 'id'.

#         Check if the highlight 'id' already exists in the database.

#         Note
#         ----
#         Book is passed to create a more helpful error message.

#         Raises
#         ------
#         ValueError
#             If the highlight 'id' already exists in the database. This should not be
#             possible since API fetches should only contain new highlights.
#         """
#         existing_highlight = (
#             self.session.query(Highlight).filter_by(id=highlight["id"]).first()
#         )
#         if existing_highlight:
#             raise ValueError(
#                 f"Highlight ID already in database: Highlight 'id'={highlight['id']},  "
#                 f"Book 'user_book_id'={book.user_book_id}"
#             )

#     def _process_highlight(self, highlight: dict[Any, Any], book: Book) -> None:
#         """Process a highlight and add it to the session.

#         Perform validation checks, field processing and add the highlight to the
#         session.
#         """
#         print(f"Highlight: {highlight['text'][:20]}")
#         self._validate_book_id(highlight, book)
#         self._validate_highlight_id(highlight, book)

#         highlight["highlighted_at"] = convert_iso_to_datetime(
#             highlight.get("highlighted_at")
#         )
#         highlight["created_at"] = convert_iso_to_datetime(highlight["created_at"])
#         highlight["updated_at"] = convert_iso_to_datetime(highlight["updated_at"])

#         # SQL Alchemy will ignore in favour of 'book' but included to be explicit.
#         highlight.pop("book_id", None)

#         highlight_obj = Highlight(**highlight, book=book)
#         self.session.add(highlight_obj)


# def query_get_last_fetch(session: Session) -> datetime | None:
#     """Get the last fetch."""
#     stmt = (
#         select(ReadwiseBatch).order_by(desc(ReadwiseBatch.database_write_time)).limit(1)
#     )
#     result = session.execute(stmt).scalars().first()
#     if result:
#         return result.database_write_time
#     else:
#         return None


def query_db_tables(session: Session) -> list[str] | None:
    """Test query for getting a result back."""
    inspector = inspect(session.bind)
    tables = inspector.get_table_names()
    print("Tables in the database:", tables)
    return tables


def query_books_table(session: Session) -> list[Book] | None:
    """Test query for getting a result back."""
    stmt = select(Book).limit(10)
    result = session.execute(stmt).scalars().all()
    print("=== TEST DATA ===")
    for book in result:
        print(book.title, book.author)
    print("==================")
    return result


# def query_books_table_tweets(session: Session) -> None:
#     """Test query for getting a result back."""
#     stmt = select(Book).where(Book.category == "tweets").limit(10)
#     result = session.execute(stmt).scalars().all()
#     print("=== TEST DATA ===")
#     for book in result:
#         print(book.category, book.title)
#     print("==================")
