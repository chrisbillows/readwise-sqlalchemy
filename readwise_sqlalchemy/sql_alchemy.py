import json
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional

from sqlalchemy import (
    ForeignKey,
    String,
    Text,
    create_engine,
    desc,
    inspect,
    select,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
    sessionmaker,
)
from sqlalchemy.types import TypeDecorator


def convert_to_datetime(date_str: Any | None) -> datetime | None:
    """Convert an ISO 8601 string to a datetime object."""
    return datetime.fromisoformat(date_str.replace("Z", "+00:00")) if date_str else None


class JSONEncodedList(TypeDecorator[list[dict[Any, Any]]]):
    """Encode and decode a list of dictionaries stored as a JSON string.

    Note
    ----
    `Dialect` is required by the `sqlalchemy.TypeDecorator`.
    """

    impl = Text

    def process_bind_param(self, value: Any | None, dialect: Any) -> str | None:
        if value is None:
            return None
        if not isinstance(value, list):
            raise ValueError(f"Expected a list, got {type(value)}")
        return ",".join(value)

    def process_result_value(
        self, value: str | None, dialect: Any
    ) -> list[dict[str, str]] | None:
        return json.loads(value) if value is not None else []


class CommaSeparatedList(TypeDecorator[list[str]]):
    """Encode and a decode a list stored as a comma-separated string.

    Note
    ----
    `Dialect` is required by the `sqlalchemy.TypeDecorator`.
    """

    impl = String

    def process_bind_param(self, value: list[str] | None, dialect: Any) -> str | None:
        if value is None:
            return None
        return ",".join(value)

    def process_result_value(self, value: str | None, dialect: Any) -> list[str] | None:
        if value is None:
            return None
        return value.split(",")


class Base(DeclarativeBase):
    __allow_unmapped__ = True


class Book(Base):
    __tablename__ = "books"

    user_book_id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(nullable=False)
    author: Mapped[Optional[str]] = mapped_column(nullable=True)
    readable_title: Mapped[Optional[str]] = mapped_column(nullable=True)
    source: Mapped[Optional[str]] = mapped_column(nullable=True)
    cover_image_url: Mapped[Optional[str]] = mapped_column(nullable=True)
    unique_url: Mapped[Optional[str]] = mapped_column(nullable=True)
    summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    category: Mapped[Optional[str]] = mapped_column(nullable=True)
    document_note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    readwise_url: Mapped[Optional[str]] = mapped_column(nullable=True)
    source_url: Mapped[Optional[str]] = mapped_column(nullable=True)
    asin: Mapped[Optional[str]] = mapped_column(nullable=True)
    book_tags: Mapped[Optional[List[str]]] = mapped_column(
        CommaSeparatedList, nullable=True
    )

    highlights: Mapped[List["Highlight"]] = relationship(back_populates="book")


class Highlight(Base):
    __tablename__ = "highlights"

    id: Mapped[int] = mapped_column(primary_key=True)
    book_id: Mapped[int] = mapped_column(
        ForeignKey("books.user_book_id"), nullable=False
    )
    text: Mapped[str] = mapped_column(nullable=False)
    location: Mapped[Optional[int]] = mapped_column(nullable=True)
    location_type: Mapped[Optional[str]] = mapped_column(nullable=True)
    note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    color: Mapped[Optional[str]] = mapped_column(nullable=True)
    highlighted_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    updated_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    external_id: Mapped[Optional[str]] = mapped_column(nullable=True)
    end_location: Mapped[Optional[int]] = mapped_column(nullable=True)
    url: Mapped[Optional[str]] = mapped_column(nullable=True)
    is_favorite: Mapped[bool] = mapped_column(default=False)
    is_discard: Mapped[bool] = mapped_column(default=False)
    readwise_url: Mapped[Optional[str]] = mapped_column(nullable=True)
    tags: Mapped[Optional[List[dict[Any, Any]]]] = mapped_column(
        JSONEncodedList, nullable=True
    )

    book: Mapped["Book"] = relationship(back_populates="highlights")


class ReadwiseBatches(Base):
    __tablename__ = "readwise_batches"

    batch_id: Mapped[int] = mapped_column(primary_key=True)
    start_time: Mapped[datetime] = mapped_column(nullable=False)
    end_time: Mapped[datetime] = mapped_column(nullable=False)
    database_write_time: Mapped[datetime] = mapped_column(nullable=False)


def create_database(database_path: str | Path) -> None:
    """Create the database schema. This should only be called during setup."""
    engine = create_engine(f"sqlite:///{database_path}")
    Base.metadata.create_all(engine)


def get_session(database_path: str | Path) -> Session:
    """Establish a connection to the database and return a session."""
    engine = create_engine(f"sqlite:///{database_path}")
    Session = sessionmaker(bind=engine)
    return Session()


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
        self._process_batch()
        print("Populating database")

        for book in self.books:
            highlights_data = book.pop("highlights", [])
            book_obj = self._process_book(book)
            for highlight in highlights_data:
                self._process_highlight(highlight, book_obj)

        try:
            print("Committing session")
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            print(f"Error occurred: {e}")

    def _process_batch(self) -> None:
        """Add the ReadwiseBatch to the session."""
        new_batch = ReadwiseBatches(
            start_time=self.start_fetch,
            end_time=self.end_fetch,
            database_write_time=datetime.now(),
        )
        self.session.add(new_batch)

    def _process_book(self, book: dict[Any, Any]) -> Book:
        """Process a book.

        Checks if a book exists in the database and, if it does not, adds it to the
        session.

        Returns
        -------
        Book
            The book object that either already exists in the database or has been
            added to the session.
        """
        print("Book title:", book["title"])
        existing_book = (
            self.session.query(Book)
            .filter_by(user_book_id=book["user_book_id"])
            .first()
        )
        if not existing_book:
            print("Book not in database")
            new_book = Book(**book)
            self.session.add(new_book)
            self.session.flush()
            print(f"Added new book: {book['title']}, ID: {new_book.user_book_id}")
            return new_book
        else:
            print(f"Book with ID {book['user_book_id']} already exists")
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
                f"Book 'user_book_id'={highlight['user_book_id']}"
            )

    def _process_highlight(self, highlight: dict[Any, Any], book: Book) -> None:
        """Process a highlight and add it to the session.

        Perform validation checks, field processing and add the highlight to the
        session.
        """
        print(f"Highlight: {highlight['text'][:20]}")
        self._validate_book_id(highlight, book)
        self._validate_highlight_id(highlight, book)

        highlight["highlighted_at"] = convert_to_datetime(
            highlight.get("highlighted_at")
        )
        highlight["created_at"] = convert_to_datetime(highlight["created_at"])
        highlight["updated_at"] = convert_to_datetime(highlight["updated_at"])

        # SQL Alchemy will ignore in favour of 'book' but included to be explicit.
        highlight.pop("book_id", None)

        highlight_obj = Highlight(**highlight, book=book)
        self.session.add(highlight_obj)


def query_get_last_fetch(session: Session) -> datetime | None:
    """Get the last fetch."""
    stmt = (
        select(ReadwiseBatches)
        .order_by(desc(ReadwiseBatches.database_write_time))
        .limit(1)
    )
    result = session.execute(stmt).scalars().first()
    if result:
        return result.database_write_time
    else:
        return None


def query_database_tables(session: Session) -> None:
    """Test query for getting a result back."""
    if session.bind is None:
        raise ValueError("The session is not bound to an engine or connection.")
    inspector = inspect(session.bind)
    tables = inspector.get_table_names()
    print("Tables in the database:", tables)


def query_books_table(session: Session) -> None:
    """Test query for getting a result back."""
    stmt = select(Book).limit(10)
    result = session.execute(stmt).scalars().all()
    print("=== TEST DATA ===")
    for book in result:
        print(book.title, book.author)
    print("==================")


def query_books_table_tweets(session: Session) -> None:
    """Test query for getting a result back."""
    stmt = select(Book).where(Book.category == "tweets").limit(10)
    result = session.execute(stmt).scalars().all()
    print("=== TEST DATA ===")
    for book in result:
        print(book.category, book.title)
    print("==================")
