from datetime import datetime
import json

from sqlalchemy import (
    create_engine,
    inspect,
    Column,
    Integer,
    String,
    Text,
    ForeignKey,
    DateTime,
    Boolean,
    select,
    desc,
)
from sqlalchemy.orm import relationship, sessionmaker, Session, declarative_base
from sqlalchemy.types import TypeDecorator

Base = declarative_base()


def convert_to_datetime(date_str):
    """Converts ISO 8601 string to a datetime object. Returns None if input is None."""
    return datetime.fromisoformat(date_str.replace("Z", "+00:00")) if date_str else None


class CommaSeparatedList(TypeDecorator):
    """Converts a list to a comma-separated string for storage, and back to a list when retrieved."""

    impl = String

    def process_bind_param(self, value, dialect):
        # Convert list to comma-separated string before storing
        return ",".join(value) if isinstance(value, list) else value

    def process_result_value(self, value, dialect):
        # Convert comma-separated string back to list when retrieving
        return value.split(",") if value else []


class JSONEncodedList(TypeDecorator):
    """Converts a list of dictionaries to a JSON string for storage, and back to a list when retrieved."""

    impl = Text

    def process_bind_param(self, value, dialect):
        # Convert the list of dicts to JSON string before storing
        return json.dumps(value) if value is not None else None

    def process_result_value(self, value, dialect):
        # Convert JSON string back to list of dicts when retrieving
        return json.loads(value) if value is not None else []


class Book(Base):
    """Create the `'books'` table."""

    __tablename__ = "books"

    user_book_id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    author = Column(String, nullable=True)
    readable_title = Column(String, nullable=True)
    source = Column(String, nullable=True)
    cover_image_url = Column(String, nullable=True)
    unique_url = Column(String, nullable=True)
    summary = Column(Text, nullable=True)
    category = Column(String, nullable=True)
    document_note = Column(Text, nullable=True)
    readwise_url = Column(String, nullable=True)
    source_url = Column(String, nullable=True)
    asin = Column(String, nullable=True)
    book_tags = Column(CommaSeparatedList, nullable=True)

    highlights = relationship("Highlight", back_populates="book")


class Highlight(Base):
    __tablename__ = "highlights"

    id = Column(Integer, primary_key=True)
    book_id = Column(Integer, ForeignKey("books.user_book_id"), nullable=False)
    text = Column(Text, nullable=False)
    location = Column(Integer, nullable=True)
    location_type = Column(String, nullable=True)
    note = Column(Text, nullable=True)
    color = Column(String, nullable=True)
    highlighted_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)
    external_id = Column(String, nullable=True)
    end_location = Column(Integer, nullable=True)
    url = Column(String, nullable=True)
    is_favorite = Column(Boolean, default=False)
    is_discard = Column(Boolean, default=False)
    readwise_url = Column(String, nullable=True)
    tags = Column(JSONEncodedList, nullable=True)

    book = relationship("Book", back_populates="highlights")


class ReadwiseBatches(Base):
    __tablename__ = "readwise_batches"

    batch_id = Column(Integer, primary_key=True)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    database_write_time = Column(DateTime, nullable=False)


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
        books: list[dict],
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

    def process_book(self, book: dict):
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

    def process_highlight(self, highlight: dict):
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


def query_get_last_fetch(session: Session) -> datetime:
    stmt = (
        select(ReadwiseBatches)
        .order_by(desc(ReadwiseBatches.database_write_time))
        .limit(1)
    )
    result = session.execute(stmt).scalars().first()
    return result.database_write_time


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


def query_database_tables(session: Session):
    inspector = inspect(session.bind)
    tables = inspector.get_table_names()
    print("Tables in the database:", tables)


def query_books_table(session: Session):
    books = session.query(Book).all()
    print(books)
    