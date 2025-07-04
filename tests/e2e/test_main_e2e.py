import json
import logging
import random
import sys
from dataclasses import dataclass
from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from readwise_local_plus.cli import main
from readwise_local_plus.config import UserConfig, fetch_user_config
from readwise_local_plus.db_operations import get_session
from readwise_local_plus.models import (
    Book,
    BookTag,
    Highlight,
    HighlightTag,
    ReadwiseBatch,
)

logger = logging.getLogger(__name__)


E2E_TEST_DATA_PATH = fetch_user_config().app_dir / "my_readwise_highlights.json"

pytestmark = pytest.mark.skipif(
    not E2E_TEST_DATA_PATH.exists(),
    reason="Skipping module: 'my_readwise_highlights.json' not found",
)


# ---------
#  Helpers
# ---------


@dataclass
class UsersReadwiseData:
    """
    Container for a User's Readwise Highlights for testing.
    """

    full_content: dict
    total_books: int
    total_book_tags: int
    total_highlights: int
    total_highlight_tags: int


def get_users_readwise_data() -> UsersReadwiseData:
    """
    Fetch a container of user's readwise content and related data.

    Returns
    -------
    UsersReadwiseData
        An object containing the content of the user's readwise highlights database
        and associated statistics, for testing.
    """
    with open(E2E_TEST_DATA_PATH) as file_handle:
        full_content = json.load(file_handle)

    total_books = len(full_content)
    total_book_tags = sum(len(book["book_tags"]) for book in full_content)
    total_highlights = sum(len(book["highlights"]) for book in full_content)
    total_highlight_tags = sum(
        len(highlight["tags"])
        for book in full_content
        for highlight in book["highlights"]
    )

    users_readwise_data = UsersReadwiseData(
        full_content,
        total_books=total_books,
        total_book_tags=total_book_tags,
        total_highlights=total_highlights,
        total_highlight_tags=total_highlight_tags,
    )
    return users_readwise_data


def find_a_sample_book_tag(
    readwise_api_data: list[dict[str, Any]],
) -> tuple[dict, dict]:
    """
    Find a book tag to use as test sample (many books may not be tagged).

    Parameters
    ----------
    readwise_api_data: dict
        A list of dictionaries where each dictionary represents a book with highlights
        e.g. the standard Readwise API Highlight export response format.

    Returns
    -------
    tuple[dict, dict]
        A tuple of the book the book tag is connected to, and the book tag itself.
    """
    books_with_tags = [book for book in readwise_api_data if book.get("book_tags")]

    if not books_with_tags:
        return None, None

    random.seed(5)
    sample_book = random.choice(books_with_tags)
    sample_book_tag = random.choice(sample_book["book_tags"])

    return sample_book, sample_book_tag


def find_a_sample_highlight_tag(
    readwise_api_data: list[dict[str, Any]],
) -> tuple[dict, dict, dict]:
    """
    Find a highlight tag to use as test sample (many highlights may not be tagged).

    Parameters
    ----------
    readwise_api_data: dict
        A list of dictionaries where each dictionary represents a book with highlights
        e.g. the standard Readwise API Highlight export response format.

    Returns
    -------
    tuple[dict, dict, dict]
        A tuple of the book and highlight the tag is connected to, and the highlight tag
        itself.
    """

    # Every book should have highlights.
    highlights_with_tags = [
        (hl, book)
        for book in readwise_api_data
        for hl in book["highlights"]
        if hl.get("tags")
    ]

    if not highlights_with_tags:
        return None, None, None

    random.seed(3)
    sample_hl, sample_book = random.choice(highlights_with_tags)
    sample_hl_tag = random.choice(sample_hl["tags"])

    return sample_book, sample_hl, sample_hl_tag


# --------------
#  Test Helpers
# --------------


def test_find_a_book_tag():
    mock_rw_api_content = [
        {"title": "x", "book_tags": []},
        {"title": "y", "book_tags": [{"id": 1, "name": "one"}]},
    ]
    book, book_tag = find_a_sample_book_tag(mock_rw_api_content)
    # Check both are falsy as expected behaviour is both will be falsy or neither.
    if not book and not book_tag:
        pytest.skip("No books with tags found in Readwise user data")

    assert book == mock_rw_api_content[1]
    assert book_tag == mock_rw_api_content[1]["book_tags"][0]


def test_find_a_highlight_tag():
    mock_rw_api_content = [
        {
            "title": "x",
            "highlights": [{"highlight": "x", "tags": [{"id": 1, "name": "tag_name"}]}],
        }
    ]
    actual_book, actual_hl, actual_hl_tag = find_a_sample_highlight_tag(
        mock_rw_api_content
    )
    # Check all are falsy as expected behaviour is all are falsy or none are.
    if not actual_book and not actual_hl and not actual_hl_tag:
        pytest.skip("No books with tags found in Readwise user data")

    assert actual_book == mock_rw_api_content[0]
    assert actual_hl == mock_rw_api_content[0]["highlights"][0]
    assert actual_hl_tag == mock_rw_api_content[0]["highlights"][0]["tags"][0]


# ----------
#  Fixtures
# ----------


@pytest.fixture(scope="module")
@patch("readwise_local_plus.pipeline.fetch_from_export_api")
def initial_populate_of_db_from_user_data(
    mock_fetch_from_export_api: MagicMock,
    mock_user_config_module_scoped: UserConfig,
) -> tuple[UsersReadwiseData, Session]:
    """
    Write initial fetch to a new tmp_dir database via the ``mock_user_config`` fixture.
    """

    rw_data = get_users_readwise_data()
    mock_fetch_from_export_api.return_value = rw_data.full_content

    sys.argv = ["rw", "sync", "--delta"]
    main(mock_user_config_module_scoped)

    # Cheeky assert just as a double check while passing through.
    mock_fetch_from_export_api.assert_called_once_with(None)

    session = get_session(mock_user_config_module_scoped.db_path)
    return rw_data, session


# -----------------------------
#  Tests (all against fixture)
# -----------------------------


def test_total_books(
    initial_populate_of_db_from_user_data: tuple[UsersReadwiseData, Session],
):
    rw_data, session = initial_populate_of_db_from_user_data
    stmt = select(func.count()).select_from(Book)
    actual_total_books = session.execute(stmt).scalar()

    assert actual_total_books == rw_data.total_books


def test_sample_book(
    initial_populate_of_db_from_user_data: tuple[UsersReadwiseData, Session],
):
    rw_data, session = initial_populate_of_db_from_user_data
    random.seed(1)
    sample_book = random.choice(rw_data.full_content)
    stmt = select(Book).where(Book.title == sample_book["title"])
    result = session.execute(stmt).scalars().all()

    # Check only 1 item matches query.
    assert len(result) == 1

    fetched_book = result[0]
    assert fetched_book.title == sample_book["title"]
    assert len(fetched_book.highlights) == len(sample_book["highlights"])


def test_total_book_tags(
    initial_populate_of_db_from_user_data: tuple[UsersReadwiseData, Session],
):
    rw_data, session = initial_populate_of_db_from_user_data

    stmt = select(func.count()).select_from(BookTag)
    actual_total_book_tags = session.execute(stmt).scalar()

    assert actual_total_book_tags == rw_data.total_book_tags


def test_sample_book_tag(
    initial_populate_of_db_from_user_data: tuple[UsersReadwiseData, Session],
):
    rw_data, session = initial_populate_of_db_from_user_data

    # Find a highlight with tags.
    sample_book, sample_book_tag = find_a_sample_book_tag(rw_data.full_content)
    if sample_book is None and sample_book_tag is None:
        pytest.skip("No books with tags found in Readwise user data")

    stmt = select(BookTag).where(BookTag.id == sample_book_tag["id"])
    result = session.execute(stmt).scalars().all()

    # Check only 1 item matches query.
    assert len(result) == 1

    fetched_book_tag = result[0]
    assert fetched_book_tag.name == sample_book_tag["name"]
    assert fetched_book_tag.id == sample_book_tag["id"]
    assert fetched_book_tag.book.user_book_id == sample_book["user_book_id"]


def test_total_highlights(
    initial_populate_of_db_from_user_data: tuple[UsersReadwiseData, Session],
):
    rw_data, session = initial_populate_of_db_from_user_data

    stmt = select(func.count()).select_from(Highlight)
    actual_total_highlights = session.execute(stmt).scalar()

    assert actual_total_highlights == rw_data.total_highlights


def test_sample_highlight(
    initial_populate_of_db_from_user_data: tuple[UsersReadwiseData, Session],
):
    rw_data, session = initial_populate_of_db_from_user_data
    random.seed(2)
    sample_book = random.choice(rw_data.full_content)
    sample_highlight = random.choice(sample_book["highlights"])

    stmt = select(Highlight).where(Highlight.id == sample_highlight["id"])
    result = session.execute(stmt).scalars().all()

    # Check only 1 item matches query.
    assert len(result) == 1

    fetched_highlight = result[0]
    assert fetched_highlight.text == sample_highlight["text"]


def test_total_readwise_batches(
    initial_populate_of_db_from_user_data: tuple[UsersReadwiseData, Session],
):
    _, session = initial_populate_of_db_from_user_data
    stmt = select(func.count()).select_from(ReadwiseBatch)
    actual_readwise_batches = session.execute(stmt).scalar()
    assert actual_readwise_batches == 1


def test_sample_readwise_batch(
    initial_populate_of_db_from_user_data: tuple[UsersReadwiseData, Session],
):
    rw_data, session = initial_populate_of_db_from_user_data

    stmt = select(ReadwiseBatch)
    result = session.execute(stmt).scalars().all()

    # Check only 1 item matches query.
    assert len(result) == 1

    fetched_batch = result[0]
    assert fetched_batch.id == 1
    assert len(fetched_batch.books) == rw_data.total_books
    assert len(fetched_batch.highlights) == rw_data.total_highlights
    assert len(fetched_batch.highlight_tags) == rw_data.total_highlight_tags
    # Not mocked so should be today's date unless ran at midnight.
    assert fetched_batch.start_time.date() == date.today(), (
        "Expected to fail around midnight..."
    )


def test_total_highlight_tags(
    initial_populate_of_db_from_user_data: tuple[UsersReadwiseData, Session],
):
    rw_data, session = initial_populate_of_db_from_user_data

    stmt = select(func.count()).select_from(HighlightTag)
    actual_total_highlights = session.execute(stmt).scalar()

    assert actual_total_highlights == rw_data.total_highlight_tags


def test_sample_highlight_tag(
    initial_populate_of_db_from_user_data: tuple[UsersReadwiseData, Session],
):
    rw_data, session = initial_populate_of_db_from_user_data

    # Find a highlight with tags.
    _, sample_hl, sample_hl_tag = find_a_sample_highlight_tag(rw_data.full_content)

    stmt = select(HighlightTag).where(HighlightTag.id == sample_hl_tag["id"])
    result = session.execute(stmt).scalars().all()

    # Check only 1 item matches query.
    assert len(result) == 1

    fetched_hl_tag = result[0]
    assert fetched_hl_tag.name == sample_hl_tag["name"]
    assert fetched_hl_tag.highlight_id == sample_hl["id"]
