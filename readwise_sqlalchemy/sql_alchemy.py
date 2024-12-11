from datetime import datetime
import json
from typing import Any, List, Optional

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Text,
    ForeignKey,
    DateTime,
    Boolean,
    select,
    desc,
    inspect,
)
from sqlalchemy.orm import (
    Mapped,
    mapped_column,
    relationship,
    DeclarativeBase,
    Session,
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

    def process_result_value(self, value: str| None, dialect: Any) -> list[dict[str, str]] | None:
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
    book_tags: Mapped[Optional[List[str]]] = mapped_column(CommaSeparatedList, nullable=True)

    highlights: Mapped[List["Highlight"]] = relationship(back_populates="book")


class Highlight(Base):
    __tablename__ = "highlights"

    id: Mapped[int] = mapped_column(primary_key=True)
    book_id: Mapped[int] = mapped_column(ForeignKey("books.user_book_id"), nullable=False)
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
    tags: Mapped[Optional[List[dict[Any, Any]]]] = mapped_column(JSONEncodedList, nullable=True)

    book: Mapped["Book"] = relationship(back_populates="highlights")


class ReadwiseBatches(Base):
    __tablename__ = "readwise_batches"

    batch_id: Mapped[int] = mapped_column(primary_key=True)
    start_time: Mapped[datetime] = mapped_column(nullable=False)
    end_time: Mapped[datetime] = mapped_column(nullable=False)
    database_write_time: Mapped[datetime] = mapped_column(nullable=False)


def create_database(database_path: str) -> None:
    """
    Create the database schema. This should only be called during setup.
    """
    engine = create_engine(f"sqlite:///{database_path}")
    Base.metadata.create_all(engine)


def get_session(database_path: str) -> Session:
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
        self.session = session
        self.books = books
        self.start_fetch = start_fetch
        self.end_fetch = end_fetch

    # TODO: Work in progress.
    # def driver(self):
    #     self.initiate_batch()
    #     for book in self.books:
    #         highlights_data = book.pop("highlights", [])
    # book_data = self.process_book(book, Session)
    # for highlight in highlights_data:
    #     processed_highlight = self.process_highlight(highlight)

    # TODO: Work in progress.
    # def initiate_batch(self):
    #     """Create and maybe also close batch???"""
    # new_batch = ReadwiseBatches(
    #    start_time=start_fetch,
    #    end_time=end_fetch,
    #    database_write_time=datetime.now()
    # )
    # session.add(new_batch)

    def process_book(self, book: dict[Any, Any]) -> None:
        print(book["title"])
        existing_book = (
            self.session.query(Book)
            .filter_by(user_book_id=book["user_book_id"])
            .first()
        )
        if not existing_book:
            print("Book not existing")
            book_data = Book(**book)
            self.session.add(book_data)
            self.session.flush()
            print(f"Added new book: {book['title']}, ID: {book_data.user_book_id}")
        else:
            book_data = existing_book
            print(
                f"Book with ID {book['user_book_id']} already exists, adding new highlights..."
            )

    def process_highlight(self, highlight: dict[Any, Any]) -> dict[Any, Any]:
        highlight["highlighted_at"] = convert_to_datetime(
            highlight.get("highlighted_at")
        )
        highlight["created_at"] = convert_to_datetime(highlight.get("created_at"))
        highlight["updated_at"] = convert_to_datetime(highlight.get("updated_at"))
        highlight.pop("book_id")
        return highlight


# def populate_database(
#     session: Session,
#     books: list[dict],
#     start_fetch: datetime,
#     end_fetch: datetime
#     ):
#     for book in books:
#         highlights_data = book.pop("highlights", [])
#         book_data = process_book(book, Session)
#         for highlight in highlights_data:
#             process_highlight = process_highlight

#     new_batch = ReadwiseBatches(
#        start_time=start_fetch,
#        end_time=end_fetch,
#        database_write_time=datetime.now()
#     )
#     session.add(new_batch)

#     try:
#         print("Committing session...")
#         session.commit()
#     except Exception as e:
#         session.rollback()
#         print(f"Error occurred with book_id {book_data.user_book_id}: {e}")


# def process_book(book: dict, session: Session):
#     print(book['title'])
#     existing_book = session.query(Book).filter_by(user_book_id=book["user_book_id"]).first()
#     if not existing_book:
#         print("Book not existing")
#         book_data = Book(**book)
#         session.add(book_data)
#         session.flush()
#         print(f"Added new book: {book['title']}, ID: {book_data.user_book_id}")
#     else:
#         book_data = existing_book
#         print(f"Book with ID {book['user_book_id']} already exists, adding new highlights...")


# def process_highlight(highlight: dict, session: Session):
#     highlight["highlighted_at"] = convert_to_datetime(highlight.get("highlighted_at"))
#     highlight["created_at"] = convert_to_datetime(highlight.get("created_at"))
#     highlight["updated_at"] = convert_to_datetime(highlight.get("updated_at"))
#     highlight.pop("book_id")
#     return highlight

# def test_queries(session: Session):
#     stmt = select(Book).where(Book.category == "tweets").limit(10)
#     # print(stmt)
#     chunked_iterator = session.execute(stmt)
#     for row in chunked_iterator:
#         (book, ) = row
#         print(row)
#     scalar_result = chunked_iterator.scalars()
#     for book in scalar_result:
#         print(type(book))
#         print(book.category, book.title)

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
    books = session.query(Book).all()
    print(books)
