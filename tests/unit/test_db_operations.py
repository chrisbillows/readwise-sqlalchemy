import sqlite3
from datetime import datetime
from typing import Union

import pytest
from sqlalchemy import Column, ForeignKey, Integer, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Session

from readwise_sqlalchemy.db_operations import (
    DatabasePopulaterFlattenedData,
    create_database,
    get_session,
    safe_create_sqlite_engine,
)
from readwise_sqlalchemy.main import UserConfig
from readwise_sqlalchemy.models import Book, BookTag, Highlight, HighlightTag
from tests.helpers import DbHandle, mock_api_response

START_FETCH = datetime(2025, 1, 1, 1, 0)
END_FETCH = datetime(2025, 1, 1, 1, 0)


def flatten_mock_api_response():
    """
    Manually flatten a mock book in the format expected by DatabasePopulater.

    Add validation and fk fields required by the flattened, unnested schema. The
    function takes no parameters as it's effectively a pipeline for converting the
    mock_api_response.

    This operation is done manually to decouple these tests from other logic.

    Returns
    -------
    dict
        A dictionary where keys are the objects and values are list of those objects, in
        the format expected for flattened, unnested objects. Each list has only one
        object.
    """
    mock_book = mock_api_response()[0]
    mock_book_tag = mock_book.pop("book_tags")[0]
    mock_highlight = mock_book.pop("highlights")[0]
    mock_highlight_tag = mock_highlight.pop("tags")[0]

    # Add foreign keys
    mock_book_tag["user_book_id"] = mock_book["user_book_id"]
    mock_highlight["book_id"] = mock_book["user_book_id"]
    mock_highlight_tag["highlight_id"] = mock_highlight["id"]

    # Add validation fields
    validation = {"validated": True, "validation_errors": {}}
    mock_book.update(validation)
    mock_book_tag.update(validation)
    mock_highlight.update(validation)
    mock_highlight_tag.update(validation)

    # The objects would have been pydantic validated, at which point datetime strs
    # are cast to datetime objs.
    mock_highlight["highlighted_at"] = datetime(2025, 1, 1, 0, 1)
    mock_highlight["created_at"] = datetime(2025, 1, 1, 0, 1, 10)
    mock_highlight["updated_at"] = datetime(2025, 1, 1, 0, 1, 20)

    return {
        "books": [mock_book],
        "book_tags": [mock_book_tag],
        "highlights": [mock_highlight],
        "highlight_tags": [mock_highlight_tag],
    }


def create_test_cases_from_flattened_mock_api(mock_api_response: dict):
    """
    Create test scenarios from the flattened mock api response.

    Parameters
    ----------
    mock_api_response: dict
        A flattened mock api response in the format {"books": [{book_1}, {book_2}]} etc.

    Returns
    -------
    list[tuple[str, str, str]]
        A list of tuples in format (orm_model, field, expected_value).
        E.g. ``(Book, 'user_book_id', 12345)``.
    """
    orm_models = {
        "books": Book,
        "book_tags": BookTag,
        "highlights": Highlight,
        "highlight_tags": HighlightTag,
    }
    test_cases = []
    for object_type in mock_api_response.keys():
        target_object = mock_api_response[object_type][0]
        for field, value in target_object.items():
            test_cases.append((orm_models[object_type], field, value))
    return test_cases


# def generate_list_of_objects_with_expected_fields_and_values(mock_api_response: dict):
#     """
#     Create test scenarios from mock (or real) Readwise HIGHLIGHT endpoint API response.

#     """
#     params = []
#     test_cases = [
#         (Book, lambda x: x[0].items(), ["highlights", "book_tags"]),
#         (BookTag, lambda x: x[0]["book_tags"][0].items(), []),
#         (Highlight, lambda x: x[0]["highlights"][0].items(), ["tags"]),
#         (HighlightTag, lambda x: x[0]["highlights"][0]["tags"][0].items(), []),
#     ]

#     # The following fields will be cast to datetime objects on schema verification.
#     # Update here to give the correct "expected" test values.
#     highlight = mock_api_response[0]["highlights"][0]
#     highlight["highlighted_at"] = datetime(2025, 1, 1, 0, 1)
#     highlight["created_at"] = datetime(2025, 1, 1, 0, 1, 10)
#     highlight["updated_at"] = datetime(2025, 1, 1, 0, 1, 20)

#     for orm_obj, extract_expected_fields_and_values, fields_to_ignore in test_cases:
#         expected_items = [
#             (orm_obj, field, value)
#             for field, value in extract_expected_fields_and_values(mock_api_response)
#             if field not in fields_to_ignore
#         ]
#         params.extend(expected_items)
#     return params


def test_safe_create_sqlite_engine_raises_for_a_missing_foreign_key():
    test_engine = safe_create_sqlite_engine(":memory:")

    class TestBase(DeclarativeBase):
        pass

    class Parent(TestBase):
        __tablename__ = "parent"
        id = Column(Integer, primary_key=True)

    class Child(TestBase):
        __tablename__ = "child"
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey("parent.id"))

    TestBase.metadata.create_all(test_engine)
    # We want this to raise because the parent_id doesn't exist in the db.
    with pytest.raises(IntegrityError):
        with Session(test_engine) as session, session.begin():
            session.add(Child(id=1, parent_id=999))


def test_get_session_returns_a_session_object(mock_user_config: UserConfig):
    actual = get_session(mock_user_config.db_path)
    assert isinstance(actual, Session)


def test_tables_created_by_create_database(mock_user_config: UserConfig):
    create_database(mock_user_config.db_path)
    expected = [
        ("readwise_batches",),
        ("books",),
        ("book_tags",),
        ("highlights",),
        ("highlight_tags",),
    ]
    connection = sqlite3.connect(mock_user_config.db_path)
    cursor = connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    actual = cursor.fetchall()
    connection.close()
    assert actual == expected


def test_get_session_attaches_to_a_database_url(mock_user_config: UserConfig):
    """Test the Session database url has the correct file name."""
    session = get_session(mock_user_config.db_path)
    database_url: str = session.bind.url
    actual = str(database_url).split("/")[-1]
    assert actual == "readwise.db"


# -----------------------------
# Tests - Flattened DbPopulater
# -----------------------------


def test_database_populater_flattened_instantiates_with_expected_attrs(
    mem_db: DbHandle,
):
    database_populater = DatabasePopulaterFlattenedData(
        mem_db.session, flatten_mock_api_response(), START_FETCH, END_FETCH
    )
    assert list(database_populater.__dict__.keys()) == [
        "session",
        "validated_flattened_objs",
        "start_fetch",
        "end_fetch",
    ]


@pytest.mark.parametrize(
    "orm_obj, target_field, expected_value",
    create_test_cases_from_flattened_mock_api(flatten_mock_api_response()),
)
def test_db_populater_flattened_populate_database(
    mem_db: DbHandle,
    orm_obj: Union[Book, Highlight, HighlightTag],
    target_field: str,
    expected_value: Union[str, int],
):
    validated_flattened_objs = flatten_mock_api_response()
    database_populater = DatabasePopulaterFlattenedData(
        mem_db.session, validated_flattened_objs, START_FETCH, END_FETCH
    )
    database_populater.populate_database()
    with Session(mem_db.engine) as clean_session:
        fetched_objects = clean_session.scalars(select(orm_obj)).all()
        actual_obj = fetched_objects[0]
        assert getattr(actual_obj, target_field) == expected_value


# -----------------------------
# Tests - Nested DbPopulater
# -----------------------------


# def test_database_populater_instantiates_with_expected_attrs(mem_db: DbHandle):
#     database_populater = DatabasePopulater(
#         mem_db.session, mock_api_response(), START_FETCH, END_FETCH
#     )
#     assert list(database_populater.__dict__.keys()) == [
#         "session",
#         "validated_books",
#         "start_fetch",
#         "end_fetch",
#     ]

# def test_database_populater_process_populate_database_adds_readwise_batch_to_the_session(
#     mem_db: DbHandle,
# ):
#     database_populater = DatabasePopulater(
#         mem_db.session, mock_api_response(), START_FETCH, END_FETCH
#     )
#     database_populater.populate_database()
#     expected = ReadwiseBatch(start_time=START_FETCH, end_time=END_FETCH)
#     # Better to add eq dunders, see the chat "SQLAlchemy IdentitySet Assertion Error"
#     assert any(
#         obj.start_time == expected.start_time and
#         obj.end_time == expected.end_time and
#         obj.books == expected.books
#         for obj in database_populater.session.new
#     )


# @pytest.mark.parametrize(
#     "orm_obj, target_field, expected_value",
#     generate_list_of_objects_with_expected_fields_and_values(mock_api_response()),
# )
# def test_db_populater_populate_database(
#     mem_db: DbHandle,
#     orm_obj: Union[Book, Highlight, HighlightTag],
#     target_field: str,
#     expected_value: Union[str, int],
# ):
#     validated_books = [BookSchema(**book) for book in mock_api_response()]
#     database_populater = DatabasePopulater(
#         mem_db.session, validated_books, START_FETCH, END_FETCH
#     )
#     database_populater.populate_database()
#     with Session(mem_db.engine) as clean_session:
#         fetched_objects = clean_session.scalars(select(orm_obj)).all()
#         actual_obj = fetched_objects[0]
#         assert getattr(actual_obj, target_field) == expected_value


# @pytest.mark.parametrize("orm_obj, extract_expected, field_to_ignore",
#     [
#         (Book, lambda dict: dict.items()[0], "highlights"),
#         (Highlight, lambda dict: dict.items()[0]["highlights"][0].items(), "tags"),
#     ]
# )
# def test_db_populater_populate_database_stuff(
#     mem_db: DbHandle,
#     mock_api_response: dict,
#     orm_obj: Union[Book, Highlight, HighlightTag],
#     extract_expected: Callable,
#     field_to_ignore: str,
# ):
#     database_populater = DatabasePopulater(
#         mem_db.session, mock_api_response, START_FETCH, END_FETCH
#     )
#     database_populater.populate_database()
#     with Session(mem_db.engine) as clean_session:
#         fetched_objects = clean_session.scalars(select(orm_obj)).all()
#         test_obj = fetched_objects[0]
#         expected = {
#             k:v for k,v in extract_expected(mock_api_response)
#             if k != field_to_ignore
#         }
#         for field, value in expected.items():
#             assert getattr(test_obj, field) == value


# def test_db_populater_populate_database_stuff_2(mem_db: DbHandle, mock_api_response: dict):
#     database_populater = DatabasePopulater(
#         mem_db.session, mock_api_response, START_FETCH, END_FETCH
#     )
#     database_populater.populate_database()
#     with Session(mem_db.engine) as clean_session:
#         fetched_books = clean_session.scalars(select(Book)).all()
#         test_book = fetched_books[0]
#         expected = { k:v for k,v in mock_api_response[0].items() if k != "highlights"}
#         for field, value in expected.items():
#             assert getattr(test_book, field) == value

#         # fetched_highlights = clean_session.scalars(select(Highlight)).all()
#         # fetched_tags = clean_session.scalars(select(HighlightTag)).all()

#         # fetched_batches = clean_session.scalars(select(ReadwiseBatch)).all()
#         # test_batch = fetched_batches[0]
#         # breakpoint()
#     assert 1 == 1

# @pytest.mark.parametrize(
#     "slice, expected",
#     [
#         (0, 813795626),
#         (1, 45978496),
#         (15, "https://readwise.io/open/813795626"),
#     ],
# )
# def test_populate_database_successfully_committed_highlights(self, slice, expected):
#     """Test populate database successfully commits highlights.

#     Test sample values from the first entry in the table.
#     """
#     self.dbp.populate_database()
#     connection = sqlite3.connect(self.user_config.DB)
#     cursor = connection.cursor()
#     cursor.execute("SELECT * FROM highlights")
#     actual = cursor.fetchall()[0]
#     assert actual[slice] == expected

# @pytest.mark.parametrize(
#     "slice, expected",
#     [
#         (0, 7994381),
#         (1, "Tweets From Elon Musk"),
#         (
#             5,
#             "https://pbs.twimg.com/profile_images/1858316737780781056/kPL61o0F.jpg",
#         ),
#     ],
# )
# def test_populate_database_successfully_committed_books(self, slice, expected):
#     """Test populate database successfully commits highlights.

#     Uses a sqlite3 query to test the first entry in the highlights table contains
#     the expected values at sample indexes

#     """
#     self.dbp.populate_database()
#     connection = sqlite3.connect(self.user_config.DB)
#     cursor = connection.cursor()
#     cursor.execute("SELECT * FROM books")
#     actual = cursor.fetchall()[0]
#     print(actual)
#     assert actual[slice] == expected


# def test_process_batch(self):
#     """Test adds a single ReadwiseBatch with the correct info to the session."""
#     dbp._process_batch()
#     stmt = select(ReadwiseBatch)
#     results = self.dbp.session.execute(stmt).scalars().all()
#     first_result = results[0]
#     actual = vars(first_result)
#     expected = {
#         "batch_id": 1,
#         "start_time": datetime(2025, 1, 1, 1, 0),
#         "end_time": datetime(2025, 1, 1, 1, 1),
#     }
#     assert expected.items() <= actual.items()


# def test_process_book(self):
#     """Test adding a single book is to the session."""
#     book = self.dbp.books[0]
#     book.pop("highlights", [])
#     self.dbp._process_book(book)
#     stmt = select(Book)
#     results = self.dbp.session.execute(stmt).scalars().all()
#     actual = vars(results[0])
#     expected = {"user_book_id": 46095532, "author": "Hugo Rifkind"}
#     assert expected.items() <= actual.items()


# def test_validate_a_highlights_book_id_valid(self):
#     """Validate a valid book id."""
#     book = self.dbp.books[0]
#     highlights = book.pop("highlights", [])
#     book_obj = Book(**book)
#     assert self.dbp._validate_book_id(highlights[0], book_obj) is None


# def test_validate_a_highlights_book_id_invalid(self):
#     """Attempts to validate a book id"""
#     book = self.dbp.books[0]
#     book["user_book_id"] = 123
#     highlights = book.pop("highlights", [])
#     book_obj = Book(**book)
#     with pytest.raises(ValueError):
#         self.dbp._validate_book_id(highlights[0], book_obj)


# def test_validate_highlight_id_valid(self):
#     """Validate a valid highlight id."""
#     book = self.dbp.books[0]
#     highlight = book["highlights"][0]
#     book.pop("highlights")
#     book_obj = Book(**book)
#     assert self.dbp._validate_highlight_id(highlight, book_obj) is None


# def test_validate_highlight_id_invalid(self):
#     """Attempts to validate a highlight already added to the session."""
#     book = self.dbp.books[0]
#     highlights = book.pop("highlights", [])
#     highlight = highlights[0]
#     book_obj = Book(**book)
#     # Coupled test to another method for convenience.
#     self.dbp._process_highlight(highlight, book_obj)
#     with pytest.raises(ValueError):
#         self.dbp._validate_highlight_id(highlight, book_obj)


# def test_process_highlight(self):
#     """Test adding a single highlight to the session."""
#     book = self.dbp.books[0]
#     highlights = book.pop("highlights", [])
#     highlight = highlights[0]
#     book_obj = Book(**book)
#     self.dbp._process_highlight(highlight, book_obj)
#     stmt = select(Highlight)
#     results = self.dbp.session.execute(stmt).scalars().all()
#     assert len(results) == 1
#     actual = vars(results[0])
#     expected = {
#         "id": 815092566,
#         "location": 38975,
#         "location_type": "offset",
#         "note": "",
#         "color": "",
#         "highlighted_at": datetime(
#             2024, 11, 21, 14, 7, 59, 74000, tzinfo=timezone.utc
#         ),
#         "created_at": datetime(
#             2024, 11, 21, 14, 7, 59, 102000, tzinfo=timezone.utc
#         ),
#         "updated_at": datetime(
#             2024, 11, 21, 14, 7, 59, 102000, tzinfo=timezone.utc
#         ),
#         "end_location": None,
#         "tags": [],
#         "is_favorite": False,
#         "is_discard": False,
#         "book_id": 46095532,
#     }
#     assert expected.items() <= actual.items()
