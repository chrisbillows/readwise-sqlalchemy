import json
import logging
import random
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from readwise_sqlalchemy.config import USER_CONFIG, UserConfig
from readwise_sqlalchemy.db_operations import get_session
from readwise_sqlalchemy.main import main
from readwise_sqlalchemy.models import Book, Highlight, HighlightTag, ReadwiseBatch

logger = logging.getLogger(__name__)


E2E_TEST_DATA_PATH = USER_CONFIG.app_dir / "my_readwise_highlights.json"

pytestmark = pytest.mark.skipif(
    not E2E_TEST_DATA_PATH.exists(),
    reason="Skipping module: 'my_readwise_highlights.json' not found",
)


@pytest.fixture(scope="module")
@patch("readwise_sqlalchemy.main.fetch_from_export_api")
def initial_db_install_from_user_data(
    mock_fetch_from_export_api: MagicMock, mock_user_config_module_scoped: UserConfig
):
    """
    Write initial "fetch" to a tmp_dir database via the ``mock_user_config`` fixture.
    """
    with open(E2E_TEST_DATA_PATH) as file_handle:
        api_content = json.load(file_handle)

    # TODO: Remove when fixed in issue #34.
    broken_book_title = "What to Eat to Be One of the Healthy Elite at 70"
    api_content = [book for book in api_content if book["title"] != broken_book_title]

    mock_fetch_from_export_api.return_value = api_content
    main(mock_user_config_module_scoped)

    # Cheeky assert just as a double check while passing through.
    mock_fetch_from_export_api.assert_called_once_with(None)
    session = get_session(mock_user_config_module_scoped.db_path)
    return api_content, session


def test_total_books(initial_db_install_from_user_data: tuple[dict[str, Any], Session]):
    api_content, session = initial_db_install_from_user_data

    expected_total_books = len(api_content)

    stmt = select(func.count()).select_from(Book)
    actual_total_books = session.execute(stmt).scalar()

    assert actual_total_books == expected_total_books


def test_total_highlights(
    initial_db_install_from_user_data: tuple[dict[str, Any], Session],
):
    api_content, session = initial_db_install_from_user_data
    expected_total_highlights = sum(len(book["highlights"]) for book in api_content)

    stmt = select(func.count()).select_from(Highlight)
    actual_total_highlights = session.execute(stmt).scalar()

    assert actual_total_highlights == expected_total_highlights


def test_total_highlight_tags(
    initial_db_install_from_user_data: tuple[dict[str, Any], Session],
):
    api_content, session = initial_db_install_from_user_data

    expected_total_highlights_tags = sum(
        len(highlight["tags"])
        for book in api_content
        for highlight in book["highlights"]
    )

    stmt = select(func.count()).select_from(HighlightTag)
    actual_total_highlights = session.execute(stmt).scalar()

    assert actual_total_highlights == expected_total_highlights_tags


def test_total_readwise_batches(
    initial_db_install_from_user_data: tuple[dict[str, Any], Session],
):
    api_content, session = initial_db_install_from_user_data
    stmt = select(func.count()).select_from(ReadwiseBatch)
    actual_readwise_batches = session.execute(stmt).scalar()
    assert actual_readwise_batches == 1


def test_sample_book(initial_db_install_from_user_data: tuple[dict[str, Any], Session]):
    api_content, session = initial_db_install_from_user_data
    random.seed(42)
    sample_book = random.choice(api_content)

    stmt = select(Book).where(Book.title == sample_book["title"])
    result = session.execute(stmt).scalars().all()

    # Check title appears only once in db.
    assert len(result) == 1

    fetched_book = result[0]
    assert fetched_book.title == sample_book["title"]
    assert len(fetched_book.highlights) == len(sample_book["highlights"])


def test_sample_highlight(
    initial_db_install_from_user_data: tuple[dict[str, Any], Session],
):
    api_content, session = initial_db_install_from_user_data
    pass


def test_sample_highlight_tag(
    initial_db_install_from_user_data: tuple[dict[str, Any], Session],
):
    api_content, session = initial_db_install_from_user_data
    pass


def test_sample_readwise_batch(
    initial_db_install_from_user_data: tuple[dict[str, Any], Session],
):
    api_content, session = initial_db_install_from_user_data
    pass
