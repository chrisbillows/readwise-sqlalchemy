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
    """
    Encode and decode a list of dictionaries stored as a JSON string.

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
    """
    Encode and a decode a list stored as a comma-separated string.

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
    """
    Create the database schema. This should only be called during setup.
    """
    engine = create_engine(f"sqlite:///{database_path}")
    Base.metadata.create_all(engine)


def get_session(database_path: str | Path) -> Session:
    """
    Establish a connection to the database and return a session.
    """
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
        books: list[dict[Any, Any]]
        start_fetch: datetime
        end_fetch: datetime
        """
        self.session = session
        self.books = books
        self.start_fetch = start_fetch
        self.end_fetch = end_fetch

    def populate_database(self) -> None:
        self._process_batch()
        print("Populating database")

        for book in self.books:
            # highlights_data = book.pop("highlights", [])
            book.pop("highlights", [])
            self._process_book(book)
            # for highlight in highlights_data:
            #     processed_highlight = self._process_highlight(highlight)

        try:
            print("Committing session...")
            self.session.commit()
        except Exception:
            self.session.rollback()
            print("Error occurred.")

    def _process_batch(self) -> None:
        """Add"""
        new_batch = ReadwiseBatches(
            start_time=self.start_fetch,
            end_time=self.end_fetch,
            database_write_time=datetime.now(),
        )
        self.session.add(new_batch)

    def _process_book(self, book: dict[Any, Any]) -> None:
        print("Book title:", book["title"])
        existing_book = (
            self.session.query(Book)
            .filter_by(user_book_id=book["user_book_id"])
            .first()
        )
        if not existing_book:
            print("Book not in database")
            book_data = Book(**book)
            self.session.add(book_data)
            self.session.flush()
            print(f"Added new book: {book['title']}, ID: {book_data.user_book_id}")
        else:
            book_data = existing_book
            print(f"Book with ID {book['user_book_id']} already exists")

    # def _process_highlight(self, highlight: dict[Any, Any]) -> dict[Any, Any]:
    #     print(f"Adding highlight")
    #     highlight["highlighted_at"] = convert_to_datetime(
    #         highlight.get("highlighted_at")
    #     )
    #     highlight["created_at"] = convert_to_datetime(highlight.get("created_at"))
    #     highlight["updated_at"] = convert_to_datetime(highlight.get("updated_at"))
    #     highlight.pop("book_id")
    #     return highlight


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
