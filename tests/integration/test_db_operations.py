import logging
from datetime import datetime
from typing import Union

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from readwise_local_plus.db_operations import (
    DatabasePopulaterFlattenedData,
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


def add_batch(db_path):
    session = get_session(db_path)
    with session.begin():
        batch = ReadwiseBatch(start_time=ANYTIME, end_time=ANYTIME)
        session.add(batch)
        session.flush()
        return batch, session


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


def test_book_versioning_for_a_changed_book(setup_db, mock_book):
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


def test_highlight_versioning_for_a_changed_highlight(
    setup_db, mock_book, mock_highlight
):
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


def test_book_versioning_no_changes(setup_db, mock_book):
    db_path = setup_db

    # Add original book
    batch_1, session_1 = add_batch(db_path)
    with session_1.begin():
        book = Book(**mock_book, batch=batch_1)
        session_1.add(book)

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
        assert books[0].author == "name surname"

        versions = session_check.scalars(select(BookVersion)).all()
        assert len(versions) == 0


def test_highlight_versioning_no_changes(setup_db, mock_book, mock_highlight):
    db_path = setup_db

    # Add original highlight
    batch_1, session_1 = add_batch(db_path)
    with session_1.begin():
        book = Book(**mock_book, batch=batch_1)
        highlight = Highlight(**mock_highlight, batch=batch_1)
        session_1.add_all([book, highlight])

    batch_2, session_2 = add_batch(db_path)
    dbp = DatabasePopulaterFlattenedData(
        session_2,
        {"books": [mock_book], "highlights": [mock_highlight]},
        ANYTIME,
        ANYTIME,
    )
    dbp.populate_database()

    session_check = get_session(db_path)
    with session_check.begin():
        highlights = session_check.scalars(select(Highlight)).all()
        assert len(highlights) == 1
        assert highlights[0].text == "The highlight text"

        versions = session_check.scalars(select(HighlightVersion)).all()
        assert len(versions) == 0
