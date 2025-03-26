from datetime import datetime
from typing import Any, Callable

import pytest
from sqlalchemy import Engine, inspect, select
from sqlalchemy.orm import Session

from readwise_sqlalchemy.models import (
    Book,
    CommaSeparatedList,
    Highlight,
    HighlightTag,
    ReadwiseBatch,
)
from tests.conftest import DbHandle

# Minimal object configurations.
MIN_HIGHLIGHT_1_TAG_1 = {"id": 5555, "name": "orange"}
MIN_HIGHLIGHT_1_TAG_2 = {"id": 5556, "name": "blue"}

MIN_HIGHLIGHT_1 = {"id": 111, "book_id": 99, "text": "highlight_1"}
MIN_HIGHLIGHT_2 = {"id": 222, "book_id": 99, "text": "highlight_2"}

MIN_BOOK = {"user_book_id": 99, "title": "book_1"}

START_TIME = datetime(2025, 1, 1, 10, 10, 10)
END_TIME = datetime(2025, 1, 1, 10, 10, 20)
DATABASE_WRITE_TIME = datetime(2025, 1, 1, 10, 10, 22)

# ReadwiseBatch expected to autoincrement to this value.
BATCH_ID = 1


def mock_pydantic_model_dump():
    """
    Mock output of ``<pydantic_schema>.model_dump()`` for a pydantic verified book.

    Output contains one book with one highlight. Use a function rather than a constant
    to ensure test isolation.

    Returns
    -------
    list[dict[str, Any]]
        A list of dictionaries where each dictionary is a Pydantic verified Readwise
        book with highlights.
    """
    return [
        {
            "user_book_id": 12345,
            "title": "book title",
            "author": "name surname",
            "readable_title": "book title",
            "source": "a source",
            "cover_image_url": "//link/to/image",
            "unique_url": None,
            "summary": None,
            "book_tags": [],
            "category": "books",
            "document_note": None,
            "readwise_url": "https://readwise.io/bookreview/1",
            "source_url": None,
            "asin": None,
            "highlights": [
                {
                    "id": 10,
                    "text": "The highlight text",
                    "location": 1000,
                    "location_type": "location",
                    "note": "",
                    "color": "yellow",
                    "highlighted_at": START_TIME,
                    "created_at": END_TIME,
                    "updated_at": DATABASE_WRITE_TIME,
                    "external_id": None,
                    "end_location": None,
                    "url": None,
                    "book_id": 12345,
                    "tags": [{"id": 97654, "name": "favourite"}],
                    "is_favorite": False,
                    "is_discard": False,
                    "readwise_url": "https://readwise.io/open/10",
                }
            ],
        }
    ]


# ----------
#  Fixtures
# ----------


@pytest.fixture()
def mem_db_containing_full_objects(mem_db: DbHandle):
    """
    Engine connected to an in-memory SQLite db with minimal records for all objects.

    Create a database with related entries for a book, highlight, highlight tag and a
    readwise batch.

    """
    batch = ReadwiseBatch(start_time=START_TIME, end_time=END_TIME)
    book_data = mock_pydantic_model_dump()[0]
    highlight = book_data.pop("highlights")[0]
    tag = highlight.pop("tags")[0]

    book_as_orm = Book(**book_data, batch=batch)
    highlight_as_orm = Highlight(**highlight, batch=batch)
    highlight_tag_as_orm = HighlightTag(**tag, batch=batch)

    highlight_as_orm.tags = [highlight_tag_as_orm]
    book_as_orm.highlights = [highlight_as_orm]

    with mem_db.session.begin():
        mem_db.session.add(batch)
        # Flush to generate batch id which is no nullable for other objects.
        mem_db.session.flush()
        mem_db.session.add(book_as_orm)
        batch.database_write_time = DATABASE_WRITE_TIME

    yield mem_db.engine


@pytest.fixture()
def mem_db_containing_minimal_objects(mem_db: DbHandle):
    """
    Engine connected to an in-memory SQLite db with minimal records for all objects.

    Create a database with related entries for a book, highlight, highlight tag and a
    readwise batch. The objects are created with minimal fields. All *relationship*
    fields are included to allow testing relationship configurations behave as expected
    with simplest possible objects.

    Note
    ----
    Constructing objects with minimal field is possible as SQLAlchemy ORM mapped classes
    do not enforce the presence of non-nullable fields. Missing fields will error in
    pydantic data verification.

    """
    batch = ReadwiseBatch(start_time=START_TIME, end_time=END_TIME)

    book_as_orm = Book(**MIN_BOOK)
    highlight_1 = Highlight(**MIN_HIGHLIGHT_1)
    highlight_2 = Highlight(**MIN_HIGHLIGHT_2)
    highlight_1_tag_1 = HighlightTag(**MIN_HIGHLIGHT_1_TAG_1)
    highlight_1_tag_2 = HighlightTag(**MIN_HIGHLIGHT_1_TAG_2)

    highlight_1.tags = [highlight_1_tag_1, highlight_1_tag_2]
    book_as_orm.highlights = [highlight_1, highlight_2]

    batch.books = [book_as_orm]
    batch.highlights = [highlight_1, highlight_2]
    batch.highlight_tags = [highlight_1_tag_1, highlight_1_tag_2]

    with mem_db.session.begin():
        mem_db.session.add(batch)
        # Flush to generate batch id which is no nullable for other objects.
        mem_db.session.flush()
        mem_db.session.add(book_as_orm)
        batch.database_write_time = DATABASE_WRITE_TIME
    yield mem_db.engine


@pytest.fixture()
def minimal_book_as_orm(mem_db_containing_minimal_objects: Engine):
    """A minimal ``Book`` fetched from the minimal object database."""
    with Session(mem_db_containing_minimal_objects) as clean_session:
        fetched_books = clean_session.scalars(select(Book)).all()
        test_book = fetched_books[0]
        yield test_book


@pytest.fixture()
def minimal_highlight_as_orm(mem_db_containing_minimal_objects: Engine):
    """A minimal ``Highlight`` fetched from the minimal object database."""
    with Session(mem_db_containing_minimal_objects) as clean_session:
        fetched_highlights = clean_session.scalars(select(Highlight)).all()
        test_highlight = fetched_highlights[0]
        yield test_highlight


@pytest.fixture()
def minimal_highlight_tag_as_orm(mem_db_containing_minimal_objects: Engine):
    """A minimal ``HighlightTag`` fetched from the minimal object database."""
    with Session(mem_db_containing_minimal_objects) as clean_session:
        fetched_highlight_tags = clean_session.scalars(select(HighlightTag)).all()
        test_highlight_tag = fetched_highlight_tags[0]
        yield test_highlight_tag


@pytest.fixture()
def minimal_batch_as_orm(mem_db_containing_minimal_objects: Engine):
    """A minimal ``ReadwiseBatch`` fetched from the minimal object database."""
    with Session(mem_db_containing_minimal_objects) as clean_session:
        fetched_batches = clean_session.scalars(select(ReadwiseBatch)).all()
        test_batch = fetched_batches[0]
        yield test_batch


# ----------------------
#  Testing the fixtures
# ----------------------


def test_tables_in_mem_db_containing_minimal_objects(
    mem_db_containing_minimal_objects: Engine,
):
    with Session(mem_db_containing_minimal_objects) as clean_session:
        inspector = inspect(clean_session.bind)
        tables = inspector.get_table_names()
        assert tables == ["books", "highlight_tags", "highlights", "readwise_batches"]


def test_minimal_book_as_orm_read_from_db_correctly(minimal_book_as_orm: Book):
    assert minimal_book_as_orm.user_book_id == MIN_BOOK["user_book_id"]
    assert minimal_book_as_orm.title == MIN_BOOK["title"]


def test_minimal_highlight_as_orm_read_from_db_correctly(
    minimal_highlight_as_orm: Highlight,
):
    assert minimal_highlight_as_orm.id == MIN_HIGHLIGHT_1["id"]
    assert minimal_highlight_as_orm.text == "highlight_1"


def test_minimal_highlight_tags_as_orm_read_from_db_correctly(
    minimal_highlight_tag_as_orm: HighlightTag,
):
    assert minimal_highlight_tag_as_orm.id == MIN_HIGHLIGHT_1_TAG_1["id"]
    assert minimal_highlight_tag_as_orm.name == MIN_HIGHLIGHT_1_TAG_1["name"]


def test_minimal_readwise_batch_read_from_db_correctly(
    minimal_batch_as_orm: ReadwiseBatch,
):
    assert minimal_batch_as_orm.start_time == START_TIME
    assert minimal_batch_as_orm.end_time == END_TIME
    assert minimal_batch_as_orm.database_write_time == DATABASE_WRITE_TIME


# -------
#  Tests
# -------


def test_book_relationship_with_highlights(minimal_book_as_orm: Book):
    # Relationship.
    assert len(minimal_book_as_orm.highlights) == 2
    assert isinstance(minimal_book_as_orm.highlights[0], Highlight)
    assert minimal_book_as_orm.highlights[0].id == MIN_HIGHLIGHT_1["id"]
    assert minimal_book_as_orm.highlights[0].text == MIN_HIGHLIGHT_1["text"]

    assert (
        minimal_book_as_orm.highlights[0].book.user_book_id == MIN_BOOK["user_book_id"]
    )


def test_book_relationship_with_batch(minimal_book_as_orm: Book):
    # Foreign key.
    assert minimal_book_as_orm.batch_id == BATCH_ID
    # Relationship.
    assert isinstance(minimal_book_as_orm.batch, ReadwiseBatch)
    assert minimal_book_as_orm.batch.id == BATCH_ID
    assert minimal_book_as_orm.batch.start_time == START_TIME

    assert minimal_book_as_orm.batch.books[0].user_book_id == MIN_BOOK["user_book_id"]


def test_highlight_relationship_with_book(minimal_highlight_as_orm: Highlight):
    # Foreign key.
    assert minimal_highlight_as_orm.book_id == MIN_BOOK["user_book_id"]
    # Relationship.
    assert isinstance(minimal_highlight_as_orm.book, Book)
    assert minimal_highlight_as_orm.book.user_book_id == MIN_BOOK["user_book_id"]
    assert minimal_highlight_as_orm.book.title == "book_1"

    assert minimal_highlight_as_orm.book.highlights[0].id == MIN_HIGHLIGHT_1["id"]


def test_highlight_relationship_with_highlight_tag(minimal_highlight_as_orm: Highlight):
    # Relationship.
    assert isinstance(minimal_highlight_as_orm.tags[0], HighlightTag)
    assert minimal_highlight_as_orm.tags[0].id == MIN_HIGHLIGHT_1_TAG_1["id"]
    assert minimal_highlight_as_orm.tags[0].name == "orange"

    assert minimal_highlight_as_orm.tags[0].highlight.id == MIN_HIGHLIGHT_1["id"]


def test_highlight_relationship_with_batch(minimal_highlight_as_orm: Highlight):
    # Foreign key.
    assert minimal_highlight_as_orm.batch_id == BATCH_ID
    # Relationship.
    assert isinstance(minimal_highlight_as_orm.batch, ReadwiseBatch)
    assert minimal_highlight_as_orm.batch.start_time == datetime(2025, 1, 1, 10, 10, 10)
    assert minimal_highlight_as_orm.batch.id == BATCH_ID

    assert minimal_highlight_as_orm.batch.highlights[0].id == MIN_HIGHLIGHT_1["id"]


def test_highlight_tag_relationship_with_highlight(
    minimal_highlight_tag_as_orm: HighlightTag,
):
    # Foreign key.
    assert minimal_highlight_tag_as_orm.highlight_id == MIN_HIGHLIGHT_1["id"]
    # Relationship.
    assert isinstance(minimal_highlight_tag_as_orm.highlight, Highlight)
    assert minimal_highlight_tag_as_orm.highlight.id == MIN_HIGHLIGHT_1["id"]
    assert minimal_highlight_tag_as_orm.highlight.text == "highlight_1"

    assert (
        minimal_highlight_tag_as_orm.highlight.tags[0].id == MIN_HIGHLIGHT_1_TAG_1["id"]
    )


def test_highlight_tag_relationship_with_batch(
    minimal_highlight_tag_as_orm: HighlightTag,
):
    # Foreign key.
    assert minimal_highlight_tag_as_orm.batch_id == BATCH_ID

    # Relationship.
    assert isinstance(minimal_highlight_tag_as_orm.batch, ReadwiseBatch)
    assert minimal_highlight_tag_as_orm.batch.id == BATCH_ID
    assert minimal_highlight_tag_as_orm.batch.start_time == START_TIME


def test_readwise_batch_relationship_with_book(minimal_batch_as_orm: ReadwiseBatch):
    # Relationship.
    assert len(minimal_batch_as_orm.books) == BATCH_ID
    assert isinstance(minimal_batch_as_orm.books[0], Book)
    assert minimal_batch_as_orm.books[0].user_book_id == MIN_BOOK["user_book_id"]

    assert minimal_batch_as_orm.books[0].batch_id == BATCH_ID
    assert minimal_batch_as_orm.books[0].batch.id == BATCH_ID


def test_readwise_batch_relationship_with_highlight(minimal_batch_as_orm: ReadwiseBatch):
    # Relationship.
    assert len(minimal_batch_as_orm.highlights) == 2
    assert isinstance(minimal_batch_as_orm.highlights[0], Highlight)
    assert minimal_batch_as_orm.highlights[0].id == MIN_HIGHLIGHT_1["id"]

    assert minimal_batch_as_orm.highlights[0].batch_id == BATCH_ID
    assert minimal_batch_as_orm.highlights[0].batch.id == BATCH_ID


def test_readwise_batch_relationship_with_highlight_tag(
    minimal_batch_as_orm: ReadwiseBatch,
):
    # Relationship.
    assert len(minimal_batch_as_orm.highlight_tags) == 2
    assert isinstance(minimal_batch_as_orm.highlight_tags[0], HighlightTag)
    assert minimal_batch_as_orm.highlight_tags[0].id == MIN_HIGHLIGHT_1_TAG_1["id"]

    assert minimal_batch_as_orm.highlight_tags[0].batch_id == BATCH_ID
    assert minimal_batch_as_orm.highlight_tags[0].batch.id == BATCH_ID


@pytest.mark.parametrize(
    "field, expected",
    [
        (field, value)
        for field, value in mock_pydantic_model_dump()[0].items()
        if field != "highlights"
    ],
)
def test_fetch_full_book_from_db_assert_standard_field_values(
    mem_db_containing_full_objects: Engine, field: str, expected: Any
):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_book = clean_session.get(Book, 12345)
        actual = getattr(fetched_book, field)
        assert actual == expected


def test_fetch_full_book_from_db_assert_foreign_key_values(
    mem_db_containing_full_objects: Engine,
):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_book = clean_session.get(Book, 12345)
        assert fetched_book.batch_id == 1


@pytest.mark.parametrize(
    "extract_obj_lambda, expected_type",
    [
        (lambda book: book.highlights[0], Highlight),
        (lambda book: book.batch, ReadwiseBatch),
    ],
)
def test_fetch_full_book_from_db_assert_mapped_objects(
    mem_db_containing_full_objects: Engine,
    extract_obj_lambda: Callable,
    expected_type: type,
):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_book = clean_session.get(Book, 12345)
        object_to_test = extract_obj_lambda(fetched_book)
        assert isinstance(object_to_test, expected_type)


@pytest.mark.parametrize(
    "field, expected",
    [
        (field, value)
        for field, value in mock_pydantic_model_dump()[0]["highlights"][0].items()
        if field != "tags"
    ],
)
def test_fetch_full_highlight_from_db_assert_standard_field_values(
    mem_db_containing_full_objects: Engine, field: str, expected: Any
):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_highlight = clean_session.get(Highlight, 10)
        actual = getattr(fetched_highlight, field)
        assert actual == expected


def test_fetch_full_highlight_from_db_assert_foreign_key_values(
    mem_db_containing_full_objects: Engine,
):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_highlight = clean_session.get(Highlight, 10)
        assert (
            fetched_highlight.book_id == mock_pydantic_model_dump()[0]["user_book_id"]
        )
        assert fetched_highlight.batch_id == 1


@pytest.mark.parametrize(
    "extract_obj_lambda, expected_type",
    [
        (lambda highlight: highlight.book, Book),
        (lambda highlight: highlight.tags[0], HighlightTag),
        (lambda highlight: highlight.batch, ReadwiseBatch),
    ],
)
def test_fetch_full_highlight_from_db_assert_mapped_objects(
    mem_db_containing_full_objects: Engine,
    extract_obj_lambda: Callable,
    expected_type: type,
):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_highlight = clean_session.get(Highlight, 10)
        object_to_test = extract_obj_lambda(fetched_highlight)
        assert isinstance(object_to_test, expected_type)


@pytest.mark.parametrize(
    "field, expected",
    [
        (field, value)
        for field, value in mock_pydantic_model_dump()[0]["highlights"][0]["tags"][
            0
        ].items()
    ],
)
def test_fetch_full_highlight_tag_from_db_assert_standard_field_values(
    mem_db_containing_full_objects: Engine, field: str, expected: Any
):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_highlight_tag = clean_session.get(HighlightTag, 97654)
        assert getattr(fetched_highlight_tag, field) == expected


def test_fetch_full_highlight_tag_from_db_assert_foreign_keys(mem_db_containing_full_objects: Engine):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_highlight_tag = clean_session.get(HighlightTag, 97654)
        assert fetched_highlight_tag.batch_id == 1
        assert (
            fetched_highlight_tag.highlight_id
            == mock_pydantic_model_dump()[0]["highlights"][0]["id"]
        )


@pytest.mark.parametrize(
    "extract_obj_lambda, expected_type",
    [
        (lambda highlight_tag: highlight_tag.highlight, Highlight),
        (lambda highlight_tag: highlight_tag.batch, ReadwiseBatch),
    ],
)
def test_fetch_full_highlight_tag_from_db_assert_mapped_objects(
    mem_db_containing_full_objects: Engine,
    extract_obj_lambda: Callable,
    expected_type: type,
):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_highlight_tag = clean_session.get(HighlightTag, 97654)
        object_to_test = extract_obj_lambda(fetched_highlight_tag)
        assert isinstance(object_to_test, expected_type)


@pytest.mark.parametrize(
    "field, expected",
    [
        ("start_time", START_TIME),
        ("end_time", END_TIME),
        ("database_write_time", DATABASE_WRITE_TIME),
    ],
)
def test_fetch_full_readwise_batch_from_db_assert_standard_field_values(
    mem_db_containing_full_objects: Engine, field: str, expected: Any
):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_batch = clean_session.get(ReadwiseBatch, 1)
        assert getattr(fetched_batch, field) == expected


@pytest.mark.parametrize(
    "extract_obj_lambda, expected_type",
    [
        (lambda batch: batch.books[0], Book),
        (lambda batch: batch.highlights[0], Highlight),
        (lambda batch: batch.highlight_tags[0], HighlightTag),
    ],
)
def test_fetch_full_readwise_batch_from_db_assert_mapped_objects(
    mem_db_containing_full_objects: Engine,
    extract_obj_lambda: Callable,
    expected_type: type,
):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_batch = clean_session.get(ReadwiseBatch, 1)
        object_to_test = extract_obj_lambda(fetched_batch)
        assert isinstance(object_to_test, expected_type)


@pytest.mark.parametrize(
    "target_obj, obj_id, expected",
    [
        (Book, 12345, "Book(user_book_id=12345, title='book title', highlights=1)"),
        (HighlightTag, 97654, "HighlightTag(name='favourite', id=97654)"),
        (
            Highlight,
            10,
            "Highlight(id=10, book='book title', text='The highlight text')",
        ),
        (
            ReadwiseBatch,
            1,
            "ReadwiseBatch(id=1, books=1, highlights=1, highlight_tags=1, "
            "start=2025-01-01T10:10:10, end=2025-01-01T10:10:20, "
            "write=2025-01-01T10:10:22)",
        ),
    ],
)
def test_orm_mapped_class_repr_methods_for_full_objects(
    mem_db_containing_full_objects: Engine, target_obj: type, obj_id: int, expected: str
):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_obj = clean_session.get(target_obj, obj_id)
        assert repr(fetched_obj) == expected


def test_highlight_repr_for_long_highlights(mem_db_containing_full_objects: Engine):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_highlight = clean_session.get(Highlight, 10)
        fetched_highlight.text = "This is highlight text longer than 30 characters."
        expected = (
            "Highlight(id=10, book='book title', "
            "text='This is highlight text longer ...')"
        )
        assert repr(fetched_highlight) == expected


@pytest.mark.parametrize(
    "obj, expected",
    [
        (Book, "Book(user_book_id=None, title=None, highlights=0)"),
        (Highlight, "Highlight(id=None, text=None)"),
        (HighlightTag, "HighlightTag(name=None, id=None)"),
        (
            ReadwiseBatch,
            "ReadwiseBatch(id=None, books=0, highlights=0, highlight_tags=0)",
        ),
    ],
)
def test_highlight_repr_for_empty_objects(obj: type, expected: str):
    mock_obj = obj()
    assert repr(mock_obj) == expected


class TestCommaSeparatedList:
    LIST_OF_STRINGS = ["book_tag_1", "book_tag_2"]
    STRING = "book_tag_1,book_tag_2"

    def test_process_bind_param_list_of_strings(self):
        csl = CommaSeparatedList()
        actual = csl.process_bind_param(self.LIST_OF_STRINGS, None)
        assert actual == self.STRING

    def test_process_bind_param_empty_list(self):
        csl = CommaSeparatedList()
        actual = csl.process_bind_param([], None)
        assert actual == ""

    def test_process_result_value_list_of_strings(self):
        csl = CommaSeparatedList()
        decoded = csl.process_result_value(self.STRING, None)
        assert decoded == self.LIST_OF_STRINGS

    def test_process_result_value_empty_list(self):
        csl = CommaSeparatedList()
        decoded = csl.process_result_value("", None)
        assert decoded == []


# def test_mapped_book_prevents_duplicate_user_book_ids(
#     mock_book: dict[str, Any], mem_db: DbHandle
# ):
#     book_1 = Book(**mock_book)
#     book_2 = Book(**mock_book)
#     mem_db.session.add_all([book_1, book_2])
#     with pytest.raises(IntegrityError):
#         mem_db.session.commit()


# def test_mapped_highlight_prevents_a_missing_book(
#     mock_highlight: dict[str, Any], mem_db: DbHandle
# ):
#     # NOTE: For a SQLite dialect DB, only a connection with foreign key enforcement
#     # explicitly enabled will pass.
#     mock_highlight_obj = Highlight(**mock_highlight)
#     with pytest.raises(IntegrityError, match="FOREIGN KEY constraint failed"):
#         with mem_db.session.begin():
#             mem_db.session.add(mock_highlight_obj)

#     fetched_books = result.scalars().all()
#             for book in fetched_books:
#                 print(book.__dict__)

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


# def test_convert_to_datetime_valid():
#     iso_date = "2025-01-01T00:00"
#     actual = convert_iso_to_datetime(iso_date)
#     expected = datetime(2025, 1, 1, 0, 0)
#     assert actual == expected


# def test_convert_to_datetime_invalid():
#     not_an_iso_date = "not_a_date"
#     with pytest.raises(ValueError):
#         convert_iso_to_datetime(not_an_iso_date)
