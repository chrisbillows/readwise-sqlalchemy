from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from readwise_sqlalchemy.config import UserConfig
from readwise_sqlalchemy.db_operations import get_session
from readwise_sqlalchemy.main import main
from readwise_sqlalchemy.models import Book
from tests.test_schemas import mock_api_response


@pytest.fixture()
@patch(
    "readwise_sqlalchemy.main.fetch_from_export_api", return_value=mock_api_response()
)
def run_main_with_no_existing_database(
    mock_fetch_from_export_api: MagicMock, mock_user_config: UserConfig
):
    main(mock_user_config)
    return mock_user_config


@pytest.mark.parametrize(
    "field, expected_value",
    [(k, v) for k, v in mock_api_response()[0].items() if k != "highlights"],
)
def test_main_books_table(
    field: str, expected_value: Any, run_main_with_no_existing_database: UserConfig
):
    session = get_session(run_main_with_no_existing_database.DB)
    with session:
        book = session.get(Book, 12345)
        assert getattr(book, field) == expected_value


# def test_main_highlights_table(run_main_with_no_existing_database: UserConfig):
#     pass


# def test_main_highlights_tag_table(run_main_with_no_existing_database: UserConfig):
#     pass


# def test_main_readwise_batch_table(run_main_with_no_existing_database: UserConfig):
#     pass
