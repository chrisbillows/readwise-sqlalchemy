import logging
import sqlite3
from datetime import datetime
from typing import Union

import pytest
from sqlalchemy import Column, ForeignKey, Integer, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Session

from readwise_local_plus.config import UserConfig
from readwise_local_plus.db_operations import (
    DatabasePopulaterFlattenedData,
    create_database,
    get_session,
    safe_create_sqlite_engine,
)
from readwise_local_plus.models import (
    Base,
    Book,
    BookTag,
    BookVersion,
    Highlight,
    HighlightTag,
    HighlightVersion,
    ReadwiseBatch,
)
from tests.helpers import DbHandle, flat_mock_api_response_fully_validated

logger = logging.getLogger(__name__)

# Reusable mock value
ANYTIME = datetime(2025, 1, 1, 1, 1, 1)


# ----------
#  Fixtures
# ----------


@pytest.fixture()
def mock_book() -> dict:
    return flat_mock_api_response_fully_validated()["books"][0]


@pytest.fixture()
def mock_highlight() -> dict:
    return flat_mock_api_response_fully_validated()["highlights"][0]


@pytest.fixture()
def setup_db(mock_user_config):
    # NOTE: Coupled to upstream functions for ease and table consistency, but beware
    # upstream changes that may affect tests using this fixture.
    engine = safe_create_sqlite_engine(mock_user_config.db_path, echo=False)
    Base.metadata.create_all(engine)
    return mock_user_config.db_path


# ----------
#  Helpers
# ----------


def create_test_cases_from_flattened_mock_api(flattened_mock_api_response: dict):
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
    for object_type in flattened_mock_api_response.keys():
        target_object = flattened_mock_api_response[object_type][0]
        for field, value in target_object.items():
            test_cases.append((orm_models[object_type], field, value))
    return test_cases


def add_batch(db_path):
    session = get_session(db_path)
    with session.begin():
        batch = ReadwiseBatch(start_time=ANYTIME, end_time=ANYTIME)
        session.add(batch)
        session.flush()
        return batch, session


# ----------
#  Tests
# ----------


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
        ("books",),
        ("book_tags",),
        ("book_versions",),
        ("highlights",),
        ("highlight_versions",),
        ("highlight_tags",),
        ("readwise_batches",),
    ]
    connection = sqlite3.connect(mock_user_config.db_path)
    cursor = connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    actual = cursor.fetchall()
    connection.close()
    assert sorted(actual) == sorted(expected)


def test_get_session_attaches_to_a_database_url(mock_user_config: UserConfig):
    """Test the Session database url has the correct file name."""
    session = get_session(mock_user_config.db_path)
    database_url: str = session.bind.url
    actual = str(database_url).split("/")[-1]
    assert actual == "readwise-local-plus.db"


def test_database_populater_flattened_instantiates_with_expected_attrs(
    mem_db: DbHandle,
):
    database_populater = DatabasePopulaterFlattenedData(
        mem_db.session,
        flat_mock_api_response_fully_validated(),
        ANYTIME,
        ANYTIME,
    )
    assert list(database_populater.__dict__.keys()) == [
        "session",
        "validated_flattened_objs",
        "start_fetch",
        "end_fetch",
    ]


@pytest.mark.parametrize(
    "orm_obj, target_field, expected_value",
    create_test_cases_from_flattened_mock_api(flat_mock_api_response_fully_validated()),
)
def test_db_populater_flattened_populate_database(
    mem_db: DbHandle,
    orm_obj: Union[Book, BookTag, Highlight, HighlightTag],
    target_field: str,
    expected_value: Union[str, int],
):
    validated_flattened_objs = flat_mock_api_response_fully_validated()
    database_populater = DatabasePopulaterFlattenedData(
        mem_db.session, validated_flattened_objs, ANYTIME, ANYTIME
    )
    database_populater.populate_database()
    with Session(mem_db.engine) as clean_session:
        fetched_objects = clean_session.scalars(select(orm_obj)).all()
        actual_obj = fetched_objects[0]
        assert getattr(actual_obj, target_field) == expected_value
        clean_session.close()


def test_book_versioning(setup_db, mock_book):
    db_path = setup_db

    # Add original book
    batch_1, session_1 = add_batch(db_path)
    with session_1.begin():
        book = Book(**mock_book, batch=batch_1)
        session_1.add(book)

    # Update book data
    mock_book["author"] = "Updated Author"

    batch_2, session_2 = add_batch(db_path)
    dbp = DatabasePopulaterFlattenedData(
        session_2,
        {"books": [mock_book]},
        ANYTIME,
        ANYTIME,
    )
    dbp.populate_database()

    session_check = get_session(db_path)
    with session_check.begin():
        books = session_check.scalars(select(Book)).all()
        assert len(books) == 1
        assert books[0].author == "Updated Author"

        versions = session_check.scalars(select(BookVersion)).all()
        assert len(versions) == 1
        assert versions[0].author == "name surname"


def test_highlight_versioning(setup_db, mock_book, mock_highlight):
    db_path = setup_db

    # Add original highlight
    batch_1, session_1 = add_batch(db_path)
    with session_1.begin():
        book = Book(**mock_book, batch=batch_1)
        hl = Highlight(**mock_highlight, batch=batch_1)
        session_1.add_all([book, hl])

    # Update highlight
    mock_highlight["text"] = "Updated text"

    batch_2, session_2 = add_batch(db_path)
    dbp = DatabasePopulaterFlattenedData(
        session_2,
        {"highlights": [mock_highlight]},
        ANYTIME,
        ANYTIME,
    )
    dbp.populate_database()

    session_check = get_session(db_path)
    with session_check.begin():
        highlights = session_check.scalars(select(Highlight)).all()
        assert len(highlights) == 1
        assert highlights[0].text == "Updated text"

        versions = session_check.scalars(select(HighlightVersion)).all()
        assert len(versions) == 1
        assert versions[0].text != highlights[0].text
