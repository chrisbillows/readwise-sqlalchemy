from datetime import datetime, timezone
import json
from typing import Any

from dataclasses import dataclass
from unittest import mock
import pytest
from sqlalchemy import create_engine, select, Engine, Column, Integer, ForeignKey
from sqlalchemy.exc import IntegrityError, DataError
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
import sqlite3

from .conftest import HIGHLIGHT_SCHEMA_VARIANTS, BOOK_SCHEMA_VARIANTS
from readwise_sqlalchemy.sql_alchemy import (
    Base,
    Book,
    Highlight,
    safe_create_sqlite_engine,
)


def test_safe_create_sqlite_engine():
    test_engine = safe_create_sqlite_engine(':memory:')
    class TestBase(DeclarativeBase):
        pass        
    class Parent(TestBase):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
    class Child(TestBase):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('parent.id'))
    TestBase.metadata.create_all(test_engine)
    with pytest.raises(IntegrityError):
        with Session(test_engine) as session, session.begin():
            session.add(Child(id=1, parent_id=999))


@dataclass
class DatabaseHandle:
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


@pytest.fixture
def mem_db():
    """Create an in-memory SQLite database and return an engine and session.

    Create tables for all ORM mapped classes that inherit from Base.

    A db is required to test type validation as SQLAlchemy validates data on
    engine/session commit.
    """
    engine = safe_create_sqlite_engine(":memory:")
    Base.metadata.create_all(engine)
    SessionMaker = sessionmaker(bind=engine)
    session = SessionMaker()
    yield DatabaseHandle(engine, session)
    session.close()


def test_mapped_book_for_a_valid_book(
    mock_book: dict[str, Any], mem_db: DatabaseHandle
):
    mock_book_obj = Book(**mock_book)
    with mem_db.session.begin():
        mem_db.session.add(mock_book_obj)
    with Session(mem_db.engine) as verification_session:
        fetched_book = verification_session.get(Book, 1)
    assert (fetched_book.user_book_id, fetched_book.title) == (
        mock_book_obj.user_book_id,
        mock_book_obj.title,
    )


def test_mapped_book_for_a_valid_book_retrieves_a_new_object(
    mock_book: dict[str, Any], mem_db: DatabaseHandle
):
    # SQL Alchemy caches seen objects within a session. Test the object is truly
    # retrieved from the database.
    mock_book_obj = Book(**mock_book)
    with mem_db.session.begin():
        mem_db.session.add(mock_book_obj)
    with Session(mem_db.engine) as verification_session:
        fetched_book = verification_session.get(Book, 1)
    assert fetched_book is not mock_book_obj


def test_mapped_book_allocates_primary_keys_correctly(
    mock_book: dict[str, Any], mem_db: DatabaseHandle
):
    # Create three valid mock books with different user ids.
    user_book_ids = [1000, 1999, 1001]
    mock_book_objs = []
    for user_book_id in user_book_ids:
        book_data = mock_book.copy()
        book_data["user_book_id"] = user_book_id
        mock_book_objs.append(Book(**book_data))
    with mem_db.session.begin():
        mem_db.session.add_all(mock_book_objs)
    with Session(mem_db.engine) as verification_session:
        stmt = select(Book.id, Book.title, Book.user_book_id)
        result = verification_session.execute(stmt)
        actual = result.mappings().all()
        expected = [
            {"id": 1, "title": "Example Book Title", "user_book_id": 1000},
            {"id": 2, "title": "Example Book Title", "user_book_id": 1999},
            {"id": 3, "title": "Example Book Title", "user_book_id": 1001},
        ]
    assert actual == expected


def test_mapped_book_prevents_duplicate_user_book_ids(
    mock_book: dict[str, Any], mem_db: DatabaseHandle
):
    book_1 = Book(**mock_book)
    book_2 = Book(**mock_book)
    mem_db.session.add_all([book_1, book_2])
    with pytest.raises(IntegrityError):
        mem_db.session.commit()


@pytest.mark.parametrize(
    "valid_null_field",
    [
        field
        for field in BOOK_SCHEMA_VARIANTS.keys()
        if BOOK_SCHEMA_VARIANTS[field]["nullable"]
    ],
)
def test_mapped_book_with_null_values_where_allowed(
    valid_null_field: str, mock_book: dict[str, Any], mem_db: DatabaseHandle
):
    mock_book[valid_null_field] = None
    mock_book_obj = Book(**mock_book)
    with mem_db.session.begin():
        mem_db.session.add(mock_book_obj)
    with Session(mem_db.engine) as verification_session:
        fetched_book = verification_session.get(Book, 1)
    assert (fetched_book.user_book_id, fetched_book.title) == (
        mock_book_obj.user_book_id,
        mock_book_obj.title,
    )


@pytest.mark.parametrize(
    "invalid_null_field",
    [
        field
        for field in BOOK_SCHEMA_VARIANTS.keys()
        if not BOOK_SCHEMA_VARIANTS[field]["nullable"]
    ],
)
def test_mapped_book_with_null_values_where_not_allowed(
    invalid_null_field: str, mock_book: dict[str, Any], mem_db: DatabaseHandle
):
    mock_book[invalid_null_field] = None
    mock_book_obj = Book(**mock_book)
    with pytest.raises(IntegrityError):
        with mem_db.session.begin():
            mem_db.session.add(mock_book_obj)


def test_mapped_highlight_for_a_valid_highlight(
    mock_book: dict[str, Any], mock_highlight: dict[str, Any], mem_db: DatabaseHandle
):
    mock_book_obj = Book(**mock_book)
    mock_highlight_obj = Highlight(**mock_highlight)
    with mem_db.session.begin():
        mem_db.session.add_all([mock_book_obj, mock_highlight_obj])
    with Session(mem_db.engine) as verification_session:
        fetched_highlight = verification_session.get(Highlight, 1)
    assert (fetched_highlight.text, fetched_highlight.book_id) == (
        mock_highlight_obj.text,
        mock_highlight_obj.book_id,
    )


def test_mapped_highlight_prevents_a_missing_book(
    mock_highlight: dict[str, Any], mem_db: DatabaseHandle
):
    # NOTE: For a SQLite dialect DB, only a connection with foreign key enforcement
    # explicitly enabled will pass.
    mock_highlight_obj = Highlight(**mock_highlight)
    with pytest.raises(IntegrityError, match="FOREIGN KEY constraint failed"):
        with mem_db.session.begin():
            mem_db.session.add(mock_highlight_obj)

    # fetched_books = result.scalars().all()
    #         for book in fetched_books:
    #             print(book.__dict__)

    # def test_in_memory_dict(mock_book: dict):
    #     engine = create_engine("sqlite:///:memory:", echo=True)
    #     session = sessionmaker(engine)
    #     with session.begin() as conn:
    #         conn.execute(text("CREATE TABLE some_table (x int, y int)"))
    #         conn.execute(
    #             text(
    #                 "INSERT INTO some_table (x, y) VALUES (:x, :y)"
    #                 ),
    #                 [
    #                     {"x": 1, "y": 1}, {"x": 2, "y": 4}
    #                 ],
    #          )
    #     with engine.connect() as conn:
    #         result = conn.execute(text("SELECT * FROM some_table"))
    #         for dict in result.mappings():
    #             print(dict)
    #     assert 1 == 2

    # def test_add_mock_book_to_book_table(mock_book: dict):
    # book = Book(**mock_book)
    # test_db_session.add(book)
    # test_db_session.commit()
    # assert book.user_book_id is not None

    # TODO: This may not be needed. Decide after adding pydantic.
    # @pytest.fixture
    # def empty_database(synthetic_user_config):
    #     """ """
    #     create_database(synthetic_user_config.DB)
    #     session = get_session(synthetic_user_config.DB)
    #     return session

    # TODO: This may not be needed. Decide after adding pydantic.
    # def test_schema(empty_database):
    # json_data = """
    # [
    #     {
    #         "book_with_one_highlight": {
    #             "user_book_id": 1,
    #             "title": "book title",
    #             "author": "name surname",
    #             "readable_title": "book title",
    #             "source": "a source",
    #             "cover_image_url": "//link/to/image",
    #             "unique_url": null,
    #             "summary": null,
    #             "book_tags": [],
    #             "category": "books",
    #             "document_note": null,
    #             "readwise_url": "https://readwise.io/bookreview/1",
    #             "source_url": null,
    #             "asin": null,
    #             "highlights": [
    #                 {
    #                     "id": 10,
    #                     "text": "The the highlight",
    #                     "location": 1000,
    #                     "location_type": "location",
    #                     "note": "",
    #                     "color": "yellow",
    #                     "highlighted_at": "2025-01-01T00:00:00Z",
    #                     "created_at": "2025-01-01T00:00:00Z",
    #                     "updated_at": "2025-01-01T00:00:00Z",
    #                     "external_id": null,
    #                     "end_location": null,
    #                     "url": null,
    #                     "book_id": 1,
    #                     "tags": [],
    #                     "is_favorite": false,
    #                     "is_discard": false,
    #                     "readwise_url": "https://readwise.io/open/10"
    #                 }
    #             ]
    #         }
    #     }
    # ]
    # """
    # data = json.loads(json_data)

    # with open("tests/data/real/example_books.json", "r") as file_handle:
    #     content = json.load(file_handle)['book_with_one_highlight']

    # start_fetch = datetime(2025, 1, 1, 1, 0)
    # end_fetch = datetime(2025, 1, 1, 1, 1)
    # dbp = DatabasePopulater(
    #     session, books_and_highlights, start_fetch, end_fetch
    # )
    pass


# def test_convert_to_datetime_valid():
#     iso_date = "2025-01-01T00:00"
#     actual = convert_iso_to_datetime(iso_date)
#     expected = datetime(2025, 1, 1, 0, 0)
#     assert actual == expected


# def test_convert_to_datetime_invalid():
#     not_an_iso_date = "not_a_date"
#     with pytest.raises(ValueError):
#         convert_iso_to_datetime(not_an_iso_date)


# class TestJSONEncodedList:
#     """Test JSONEncodedList decorator."""

#     JSON_LIST_OF_DICTS = [{"hl_location1": "hl_name1"}, {"hl_location2": "hl_name2"}]
#     STRING = '[{"hl_location1": "hl_name1"}, {"hl_location2": "hl_name2"}]'

#     def test_process_bind_param_valid(self):
#         """Test the JSON encoding/binding."""
#         decorator = JSONEncodedList()
#         actual = decorator.process_bind_param(self.JSON_LIST_OF_DICTS, None)
#         assert actual == self.STRING

#     def test_process_result_value_valid(self):
#         """Test the JSON decoding/result."""
#         decorator = JSONEncodedList()
#         decoded = decorator.process_result_value(self.STRING, None)
#         assert decoded == self.JSON_LIST_OF_DICTS


# class TestCommaSeparatedList:
#     """Test CommaSeparatedList decorator."""

#     JSON_LIST_OF_STRINGS = ["book_tag_1", "book_tag_2"]
#     STRING = "book_tag_1,book_tag_2"

#     def test_process_bind_param_valid(self):
#         """Test the CSL encoding/binding."""
#         decorator = CommaSeparatedList()
#         actual = decorator.process_bind_param(self.JSON_LIST_OF_STRINGS, None)
#         assert actual == self.STRING

#     def test_process_result_value_valid(self):
#         """Test the CSL decoding/result."""
#         decorator = CommaSeparatedList()
#         decoded = decorator.process_result_value(self.STRING, None)
#         assert decoded == self.JSON_LIST_OF_STRINGS


# def test_create_database_tables(synthetic_user_config):
#     """Test a database is created with the correctly named tables."""
#     create_database(synthetic_user_config.DB)
#     expected = [("books",), ("readwise_batches",), ("highlights",)]

#     connection = sqlite3.connect(synthetic_user_config.DB)
#     cursor = connection.cursor()
#     cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
#     actual = cursor.fetchall()

#     connection.close()

#     assert actual == expected


# def test_get_session(synthetic_user_config):
#     """Test a Session object is returned (which is assumed to be valid)."""
#     actual = get_session(synthetic_user_config.DB)
#     assert isinstance(actual, Session)


# def test_get_session_database_url(synthetic_user_config):
#     """Test the Session database url has the correct file name."""
#     session = get_session(synthetic_user_config.DB)
#     database_url: str = session.bind.url
#     actual = str(database_url).split("/")[-1]
#     assert actual == "readwise.db"


# # TODO: The tests current use sampled real data. This needs to be anonymised and the and
# # TODO: the tests revised to match.
# class TestDatabasePopulater:
#     """
#     Test the Database Populater class.

#     The database is created with SQLite to isolate the DatabasePopulater logic. SQLite
#     is used in tests where appropriate to further isolate the SQL Alchemy logic.
#     """

#     @pytest.fixture(autouse=True)
#     def test_setup(self, synthetic_user_config):
#         """Instantiate an object and make available to all tests in the test class.

#         Attributes
#         ----------
#         user_config: UserConfig
#             A valid UserConfig object, with the path to a database created using
#             sqlite3.
#         dbp: DatabaseProcessor
#             An instantiated DatabaseProcess object.
#         """
#         self.user_config = synthetic_user_config

#         create_database(self.user_config.DB)
#         connection = sqlite3.connect(self.user_config.DB)
#         cursor = connection.cursor()
#         cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
#         session = get_session(self.user_config.DB)
#         sample_API_data = "tests/data/real/sample_updated_books.json"
#         with open(sample_API_data, "r") as file_handle:
#             books_and_highlights = json.load(file_handle)

#         start_fetch = datetime(2025, 1, 1, 1, 0)
#         end_fetch = datetime(2025, 1, 1, 1, 1)

#         self.dbp = DatabasePopulater(
#             session, books_and_highlights, start_fetch, end_fetch
#         )

#     def test_init(self):
#         """Test the object has been instantiated with the expected attributes."""
#         assert list(self.dbp.__dict__.keys()) == [
#             "session",
#             "books",
#             "start_fetch",
#             "end_fetch",
#         ]

#     @pytest.mark.parametrize(
#         "slice, expected",
#         [
#             (0, 1),
#             (1, "2025-01-01 01:00:00.000000"),
#             (2, "2025-01-01 01:01:00.000000"),
#         ],
#     )
#     def test_populate_database_successfully_committed_readwise_batch(
#         self, slice, expected
#     ):
#         """Test populate database successfully commits the readwise batch.

#         Test sample values from the first (and only) expected entry in the table.
#         """
#         self.dbp.populate_database()
#         connection = sqlite3.connect(self.user_config.DB)
#         cursor = connection.cursor()
#         cursor.execute("SELECT * FROM readwise_batches")
#         actual = cursor.fetchall()[0]
#         assert actual[slice] == expected

#     @pytest.mark.parametrize(
#         "slice, expected",
#         [
#             (0, 813795626),
#             (1, 45978496),
#             (15, "https://readwise.io/open/813795626"),
#         ],
#     )
#     def test_populate_database_successfully_committed_highlights(self, slice, expected):
#         """Test populate database successfully commits highlights.

#         Test sample values from the first entry in the table.
#         """
#         self.dbp.populate_database()
#         connection = sqlite3.connect(self.user_config.DB)
#         cursor = connection.cursor()
#         cursor.execute("SELECT * FROM highlights")
#         actual = cursor.fetchall()[0]
#         assert actual[slice] == expected

#     @pytest.mark.parametrize(
#         "slice, expected",
#         [
#             (0, 7994381),
#             (1, "Tweets From Elon Musk"),
#             (
#                 5,
#                 "https://pbs.twimg.com/profile_images/1858316737780781056/kPL61o0F.jpg",
#             ),
#         ],
#     )
#     def test_populate_database_successfully_committed_books(self, slice, expected):
#         """Test populate database successfully commits highlights.

#         Uses a sqlite3 query to test the first entry in the highlights table contains
#         the expected values at sample indexes

#         """
#         self.dbp.populate_database()
#         connection = sqlite3.connect(self.user_config.DB)
#         cursor = connection.cursor()
#         cursor.execute("SELECT * FROM books")
#         actual = cursor.fetchall()[0]
#         print(actual)
#         assert actual[slice] == expected

#     def test_process_batch(self):
#         """Test adds a single ReadwiseBatch with the correct info to the session."""
#         self.dbp._process_batch()
#         stmt = select(ReadwiseBatch)
#         results = self.dbp.session.execute(stmt).scalars().all()
#         first_result = results[0]
#         actual = vars(first_result)
#         expected = {
#             "batch_id": 1,
#             "start_time": datetime(2025, 1, 1, 1, 0),
#             "end_time": datetime(2025, 1, 1, 1, 1),
#         }
#         assert expected.items() <= actual.items()

#     def test_process_book(self):
#         """Test adding a single book is to the session."""
#         book = self.dbp.books[0]
#         book.pop("highlights", [])
#         self.dbp._process_book(book)
#         stmt = select(Book)
#         results = self.dbp.session.execute(stmt).scalars().all()
#         actual = vars(results[0])
#         expected = {"user_book_id": 46095532, "author": "Hugo Rifkind"}
#         assert expected.items() <= actual.items()

#     def test_validate_a_highlights_book_id_valid(self):
#         """Validate a valid book id."""
#         book = self.dbp.books[0]
#         highlights = book.pop("highlights", [])
#         book_obj = Book(**book)
#         assert self.dbp._validate_book_id(highlights[0], book_obj) is None

#     def test_validate_a_highlights_book_id_invalid(self):
#         """Attempts to validate a book id"""
#         book = self.dbp.books[0]
#         book["user_book_id"] = 123
#         highlights = book.pop("highlights", [])
#         book_obj = Book(**book)
#         with pytest.raises(ValueError):
#             self.dbp._validate_book_id(highlights[0], book_obj)

#     def test_validate_highlight_id_valid(self):
#         """Validate a valid highlight id."""
#         book = self.dbp.books[0]
#         highlight = book["highlights"][0]
#         book.pop("highlights")
#         book_obj = Book(**book)
#         assert self.dbp._validate_highlight_id(highlight, book_obj) is None

#     def test_validate_highlight_id_invalid(self):
#         """Attempts to validate a highlight already added to the session."""
#         book = self.dbp.books[0]
#         highlights = book.pop("highlights", [])
#         highlight = highlights[0]
#         book_obj = Book(**book)
#         # Coupled test to another method for convenience.
#         self.dbp._process_highlight(highlight, book_obj)
#         with pytest.raises(ValueError):
#             self.dbp._validate_highlight_id(highlight, book_obj)

#     def test_process_highlight(self):
#         """Test adding a single highlight to the session."""
#         book = self.dbp.books[0]
#         highlights = book.pop("highlights", [])
#         highlight = highlights[0]
#         book_obj = Book(**book)
#         self.dbp._process_highlight(highlight, book_obj)
#         stmt = select(Highlight)
#         results = self.dbp.session.execute(stmt).scalars().all()
#         assert len(results) == 1
#         actual = vars(results[0])
#         expected = {
#             "id": 815092566,
#             "location": 38975,
#             "location_type": "offset",
#             "note": "",
#             "color": "",
#             "highlighted_at": datetime(
#                 2024, 11, 21, 14, 7, 59, 74000, tzinfo=timezone.utc
#             ),
#             "created_at": datetime(
#                 2024, 11, 21, 14, 7, 59, 102000, tzinfo=timezone.utc
#             ),
#             "updated_at": datetime(
#                 2024, 11, 21, 14, 7, 59, 102000, tzinfo=timezone.utc
#             ),
#             "end_location": None,
#             "tags": [],
#             "is_favorite": False,
#             "is_discard": False,
#             "book_id": 46095532,
#         }
#         assert expected.items() <= actual.items()
