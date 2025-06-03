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
from tests.helpers import DbHandle, flat_mock_api_response_fully_validated

# Reusable mock values
START_FETCH = datetime(2025, 1, 1, 1, 0)
END_FETCH = datetime(2025, 1, 1, 1, 0)


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


def test_database_populater_flattened_instantiates_with_expected_attrs(
    mem_db: DbHandle,
):
    database_populater = DatabasePopulaterFlattenedData(
        mem_db.session,
        flat_mock_api_response_fully_validated(),
        START_FETCH,
        END_FETCH,
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
        mem_db.session, validated_flattened_objs, START_FETCH, END_FETCH
    )
    database_populater.populate_database()
    with Session(mem_db.engine) as clean_session:
        fetched_objects = clean_session.scalars(select(orm_obj)).all()
        actual_obj = fetched_objects[0]
        assert getattr(actual_obj, target_field) == expected_value
