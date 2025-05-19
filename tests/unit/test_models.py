from copy import deepcopy
from datetime import datetime
from typing import Any, Callable

import pytest
from sqlalchemy import Engine, inspect, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from readwise_sqlalchemy.models import (
    Book,
    BookTag,
    Highlight,
    HighlightTag,
    ReadwiseBatch,
)
from tests.conftest import DbHandle

# Minimal object configurations.
MIN_HIGHLIGHT_1_TAG_2 = {"id": 5556, "name": "blue"}
MIN_HIGHLIGHT_1_TAG_1 = {"id": 5555, "name": "orange"}
MIN_HIGHLIGHT_2 = {"id": 222, "book_id": 99, "text": "highlight_2"}
MIN_HIGHLIGHT_1 = {"id": 111, "book_id": 99, "text": "highlight_1"}
MIN_BOOK_TAG_2 = {"id": 9991, "name": "book_tag_2"}
MIN_BOOK_TAG_1 = {"id": 9990, "name": "book_tag_1"}
MIN_BOOK = {"user_book_id": 99, "title": "book_1"}

VALIDATION_KEYS = {"validated": True, "validation_errors": {}}

MIN_HIGHLIGHT_1_TAG_2.update(VALIDATION_KEYS)
MIN_HIGHLIGHT_1_TAG_1.update(VALIDATION_KEYS)
MIN_HIGHLIGHT_2.update(VALIDATION_KEYS)
MIN_HIGHLIGHT_1.update(VALIDATION_KEYS)
MIN_BOOK_TAG_2.update(VALIDATION_KEYS)
MIN_BOOK_TAG_1.update(VALIDATION_KEYS)
MIN_BOOK.update(VALIDATION_KEYS)

START_TIME = datetime(2025, 1, 1, 10, 10, 10)
END_TIME = datetime(2025, 1, 1, 10, 10, 20)
DATABASE_WRITE_TIME = datetime(2025, 1, 1, 10, 10, 22)

# ReadwiseBatch expected to autoincrement to this value.
BATCH_ID = 1


def unnested_minimal_objects():
    """
    Return a dictionary of unnested minimal objects.

    Batch is included for testing convenience - "batch_id" is an auto increment PK.

    Use a function to allow for in-test mutation.

    Returns
    -------
    dict[str, dict[str, Any]]
        A dictionary of minimal unnested objects with the keys `min_book`,
        `min_book_tag`, `min_highlight`, `min_highlight_tag`, `batch_id`.
    """

    # Add foreign keys. These are added to objects when they are flattened.
    min_book_tag = deepcopy(MIN_BOOK_TAG_1)
    min_book_tag["user_book_id"] = MIN_BOOK["user_book_id"]
    min_highlight = deepcopy(MIN_HIGHLIGHT_1)
    min_highlight["book_id"] = MIN_BOOK["user_book_id"]
    min_highlight_tag = deepcopy(MIN_HIGHLIGHT_1_TAG_1)
    min_highlight_tag["highlight_id"] = MIN_HIGHLIGHT_1["id"]

    return {
        "min_book": MIN_BOOK,
        "min_book_tag": min_book_tag,
        "min_highlight": min_highlight,
        "min_highlight_tag": min_highlight_tag,
        "batch_id": BATCH_ID,
    }


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
            "is_deleted": False,
            "author": "name surname",
            "readable_title": "book title",
            "source": "a source",
            "cover_image_url": "//link/to/image",
            "unique_url": None,
            "summary": None,
            "book_tags": [
                {
                    "id": 4041,
                    "name": "book_tag",
                    "validated": True,
                    "validation_errors": {},
                }
            ],
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
                    "tags": [
                        {
                            "id": 97654,
                            "name": "favourite",
                            "validated": True,
                            "validation_errors": {},
                        }
                    ],
                    "is_favorite": False,
                    "is_discard": False,
                    "is_deleted": False,
                    "readwise_url": "https://readwise.io/open/10",
                    "validated": True,
                    "validation_errors": {},
                }
            ],
            "validation_errors": {},
            "validated": True,
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
    book_tag = book_data.pop("book_tags")[0]
    highlight = book_data.pop("highlights")[0]
    tag = highlight.pop("tags")[0]

    book_as_orm = Book(**book_data, batch=batch)
    book_tag_as_orm = BookTag(**book_tag, batch=batch)
    highlight_as_orm = Highlight(**highlight, batch=batch)
    highlight_tag_as_orm = HighlightTag(**tag, batch=batch)

    highlight_as_orm.tags = [highlight_tag_as_orm]
    book_as_orm.book_tags = [book_tag_as_orm]
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
    book_tag_1 = BookTag(**MIN_BOOK_TAG_1)
    book_tag_2 = BookTag(**MIN_BOOK_TAG_2)
    highlight_1 = Highlight(**MIN_HIGHLIGHT_1)
    highlight_2 = Highlight(**MIN_HIGHLIGHT_2)
    highlight_1_tag_1 = HighlightTag(**MIN_HIGHLIGHT_1_TAG_1)
    highlight_1_tag_2 = HighlightTag(**MIN_HIGHLIGHT_1_TAG_2)

    highlight_1.tags = [highlight_1_tag_1, highlight_1_tag_2]
    book_as_orm.highlights = [highlight_1, highlight_2]
    book_as_orm.book_tags = [book_tag_1, book_tag_2]

    batch.books = [book_as_orm]
    batch.book_tags = [book_tag_1, book_tag_2]
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
def mem_db_containing_unnested_minimal_objects(mem_db: DbHandle):
    """
    Engine with a db containing minimal object records, created from unnested data.

    Create a database with UNNESTED entries for a book, highlight, highlight tag and a
    readwise batch. Originally objects were added the db nested in a book - the tests
    therefore reflect this approach. However, nesting or unnesting the books should
    make no difference. Tests on this fixture prove this. Other tests were left using
    nested data.

    Note
    ----
    Constructing objects with minimal field is possible as SQLAlchemy ORM mapped classes
    do not enforce the presence of non-nullable fields. Missing fields will error in
    pydantic data verification.

    """
    batch = ReadwiseBatch(start_time=START_TIME, end_time=END_TIME)

    min_objs = unnested_minimal_objects()

    book_as_orm = Book(**min_objs["min_book"], batch=batch)
    book_tag_1 = BookTag(**min_objs["min_book_tag"], batch=batch)
    highlight_1 = Highlight(**min_objs["min_highlight"], batch=batch)
    highlight_1_tag_1 = HighlightTag(**min_objs["min_highlight_tag"], batch=batch)

    with mem_db.session.begin():
        mem_db.session.add(batch)
        # Flush to generate batch id which is no nullable for other objects.
        mem_db.session.flush()
        mem_db.session.add_all(
            [book_as_orm, book_tag_1, highlight_1, highlight_1_tag_1]
        )
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
def minimal_book_tag_as_orm(mem_db_containing_minimal_objects: Engine):
    """A minimal ``BookTag`` fetched from the minimal object database."""
    with Session(mem_db_containing_minimal_objects) as clean_session:
        fetched_book_tags = clean_session.scalars(select(BookTag)).all()
        test_book_tag = fetched_book_tags[0]
        yield test_book_tag


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


def test_mem_db(mem_db: DbHandle):
    with mem_db.engine.connect() as conn:
        conn.execute(text("CREATE TABLE some_table (x int, y int)"))
        conn.execute(
            text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
            [{"x": 1, "y": 1}, {"x": 2, "y": 4}],
        )
        result = conn.execute(text("SELECT * FROM some_table"))
        rows = result.all()
    assert rows == [(1, 1), (2, 4)]


def test_tables_in_mem_db_containing_minimal_objects(
    mem_db_containing_minimal_objects: Engine,
):
    with Session(mem_db_containing_minimal_objects) as clean_session:
        inspector = inspect(clean_session.bind)
        tables = inspector.get_table_names()
        assert tables == [
            "book_tags",
            "books",
            "highlight_tags",
            "highlights",
            "readwise_batches",
        ]


def test_minimal_book_as_orm_read_from_db_correctly(minimal_book_as_orm: Book):
    assert minimal_book_as_orm.user_book_id == MIN_BOOK["user_book_id"]
    assert minimal_book_as_orm.title == MIN_BOOK["title"]


def test_minimal_book_tag_as_orm_read_from_db_correctly(
    minimal_book_tag_as_orm: BookTag,
):
    assert minimal_book_tag_as_orm.id == MIN_BOOK_TAG_1["id"]
    assert minimal_book_tag_as_orm.name == MIN_BOOK_TAG_1["name"]


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


def test_tables_in_mem_db_containing_unnested_minimal_objects(
    mem_db_containing_unnested_minimal_objects: Engine,
):
    with Session(mem_db_containing_unnested_minimal_objects) as clean_session:
        inspector = inspect(clean_session.bind)
        tables = inspector.get_table_names()
        assert tables == [
            "book_tags",
            "books",
            "highlight_tags",
            "highlights",
            "readwise_batches",
        ]


def test_mem_db_containing_unnested_minimal_objects(
    mem_db_containing_unnested_minimal_objects: DbHandle,
):
    minimal_objects = unnested_minimal_objects()
    with Session(mem_db_containing_unnested_minimal_objects) as clean_session:
        fetched_books = clean_session.scalars(select(Book)).all()
        fetched_book_tags = clean_session.scalars(select(BookTag)).all()
        fetched_highlights = clean_session.scalars(select(Highlight)).all()
        fetched_highlight_tags = clean_session.scalars(select(HighlightTag)).all()
        fetched_batches = clean_session.scalars(select(ReadwiseBatch)).all()

        fetched_book = fetched_books[0]
        fetched_book_tag = fetched_book_tags[0]
        fetched_highlight = fetched_highlights[0]
        fetched_highlight_tag = fetched_highlight_tags[0]
        fetched_batch = fetched_batches[0]

        assert fetched_book.user_book_id == minimal_objects["min_book"]["user_book_id"]
        assert fetched_book_tag.id == minimal_objects["min_book_tag"]["id"]
        assert fetched_highlight.id == minimal_objects["min_highlight"]["id"]
        assert fetched_highlight_tag.id == minimal_objects["min_highlight_tag"]["id"]
        assert fetched_batch.id == minimal_objects["batch_id"]


# -------
#  Tests
# -------


def test_book_relationship_with_book_tags(minimal_book_as_orm: Book):
    # Relationship
    assert len(minimal_book_as_orm.book_tags) == 2
    assert isinstance(minimal_book_as_orm.book_tags[0], BookTag)


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


def test_book_tag_relationship_with_book(minimal_book_tag_as_orm: BookTag):
    # Foreign key.
    assert minimal_book_tag_as_orm.user_book_id == MIN_BOOK["user_book_id"]
    # Relationship.
    assert isinstance(minimal_book_tag_as_orm.book, Book)
    assert minimal_book_tag_as_orm.book.user_book_id == MIN_BOOK["user_book_id"]
    assert minimal_book_tag_as_orm.book.title == MIN_BOOK["title"]


def test_book_tag_relationship_with_batch(minimal_book_tag_as_orm: BookTag):
    # Foreign key.
    assert minimal_book_tag_as_orm.batch_id == BATCH_ID
    # Relationship.
    assert isinstance(minimal_book_tag_as_orm.batch, ReadwiseBatch)
    assert minimal_book_tag_as_orm.batch.id == BATCH_ID
    assert minimal_book_tag_as_orm.batch.start_time == START_TIME


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


def test_readwise_batch_relationship_with_book_tag(minimal_batch_as_orm: ReadwiseBatch):
    # Relationship.
    assert len(minimal_batch_as_orm.book_tags) == 2
    assert isinstance(minimal_batch_as_orm.book_tags[0], BookTag)
    assert minimal_batch_as_orm.book_tags[0].id == MIN_BOOK_TAG_1["id"]


def test_readwise_batch_relationship_with_highlight(
    minimal_batch_as_orm: ReadwiseBatch,
):
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
        if field not in {"highlights", "book_tags"}
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
        (lambda book: book.book_tags[0], BookTag),
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
        for field, value in mock_pydantic_model_dump()[0]["book_tags"][0].items()
    ],
)
def test_fetch_full_book_tag_from_db_assert_standard_field_values(
    mem_db_containing_full_objects: Engine, field: str, expected: Any
):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_book_tag = clean_session.get(BookTag, 4041)
        actual = getattr(fetched_book_tag, field)
        assert actual == expected


def test_fetch_full_book_tag_from_db_assert_foreign_key_values(
    mem_db_containing_full_objects: Engine,
):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_book_tag = clean_session.get(BookTag, 4041)
        assert (
            fetched_book_tag.user_book_id
            == mock_pydantic_model_dump()[0]["user_book_id"]
        )
        assert fetched_book_tag.batch_id == 1


@pytest.mark.parametrize(
    "extract_obj_lambda, expected_type",
    [
        (lambda book_tag: book_tag.book, Book),
        (lambda book_tag: book_tag.batch, ReadwiseBatch),
    ],
)
def test_fetch_full_book_tag_from_db_assert_mapped_objects(
    mem_db_containing_full_objects: Engine,
    extract_obj_lambda: Callable,
    expected_type: type,
):
    with Session(mem_db_containing_full_objects) as clean_session:
        fetched_book_tag = clean_session.get(BookTag, 4041)
        object_to_test = extract_obj_lambda(fetched_book_tag)
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


def test_fetch_full_highlight_tag_from_db_assert_foreign_keys(
    mem_db_containing_full_objects: Engine,
):
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
        (lambda batch: batch.book_tags[0], BookTag),
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
            "ReadwiseBatch(id=1, books=1, highlights=1, book_tags=1, highlight_tags=1, "
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
        (BookTag, "BookTag(name=None, id=None)"),
        (Highlight, "Highlight(id=None, text=None)"),
        (HighlightTag, "HighlightTag(name=None, id=None)"),
        (
            ReadwiseBatch,
            "ReadwiseBatch(id=None, books=0, highlights=0, book_tags=0, "
            "highlight_tags=0)",
        ),
    ],
)
def test_repr_for_empty_objects(obj: type, expected: str):
    mock_obj = obj()
    assert repr(mock_obj) == expected


def test_orm_mapped_book_prevents_duplicate_user_book_ids(mem_db: DbHandle):
    batch = ReadwiseBatch(start_time=START_TIME, end_time=END_TIME)

    highlight_tag_1_as_orm = HighlightTag(**MIN_HIGHLIGHT_1_TAG_1)
    highlight_1_as_orm = Highlight(**MIN_HIGHLIGHT_1, tags=[highlight_tag_1_as_orm])
    book_1 = Book(**MIN_BOOK, highlights=[highlight_1_as_orm])

    # Make different books with the same 'user_book_id'. SQL Alchemy ignores exact
    # duplicates.
    highlight_tag_2_as_orm = HighlightTag(**MIN_HIGHLIGHT_1_TAG_2)
    highlight_2_as_orm = Highlight(**MIN_HIGHLIGHT_2, tags=[highlight_tag_2_as_orm])
    MIN_BOOK["title"] = "book_2"
    book_2 = Book(**MIN_BOOK, highlights=[highlight_2_as_orm])

    batch.books = [book_1, book_2]
    batch.highlights = [highlight_1_as_orm, highlight_2_as_orm]
    batch.highlight_tags = [highlight_tag_1_as_orm, highlight_tag_2_as_orm]
    with pytest.raises(IntegrityError, match="UNIQUE constraint failed"):
        with mem_db.session:
            mem_db.session.add(batch)
            # Flush to generate batch id which is no nullable for other objects.
            mem_db.session.flush()
            mem_db.session.add_all([book_1, book_2])


def test_orm_mapped_highlight_prevents_a_missing_book(mem_db: DbHandle):
    # NOTE: For a SQLite dialect DB, only a connection with foreign key enforcement
    # explicitly enabled will pass.
    batch = ReadwiseBatch(start_time=START_TIME, end_time=END_TIME)
    highlight_tag_as_orm = HighlightTag(**MIN_HIGHLIGHT_1_TAG_1)
    highlight_as_orm = Highlight(**MIN_HIGHLIGHT_1, tags=[highlight_tag_as_orm])
    batch.highlights = [highlight_as_orm]
    batch.highlight_tags = [highlight_tag_as_orm]
    with pytest.raises(IntegrityError, match="FOREIGN KEY constraint failed"):
        with mem_db.session.begin():
            mem_db.session.add(highlight_as_orm)
