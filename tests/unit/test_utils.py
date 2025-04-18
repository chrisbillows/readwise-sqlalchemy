import json
from unittest.mock import MagicMock, patch

import pytest

from readwise_sqlalchemy.config import UserConfig
from readwise_sqlalchemy.utils import (
    create_real_user_data_json_for_end_to_end_testing,
    get_columns_and_values,
)


@pytest.mark.skip("To complete implementation")
def test_get_columns_and_values(minimal_book_as_orm):
    actual = get_columns_and_values(minimal_book_as_orm)
    expected = {
        "user_book_id": 99,
        "title": "book_2",
        "author": None,
        "readable_title": None,
        "source": None,
        "cover_image_url": None,
        "unique_url": None,
        "summary": None,
        "book_tags": [],
        "category": None,
        "document_note": None,
        "readwise_url": None,
        "source_url": None,
        "asin": None,
        "batch_id": 1,
    }
    assert actual == expected


@patch("readwise_sqlalchemy.utils.fetch_from_export_api")
def test_create_real_user_data_json_for_end_to_end_testing(
    mock_fetch_from_export_api: MagicMock, mock_user_config: UserConfig
):
    mock_books = ["book_1", "book_2"]
    mock_fetch_from_export_api.return_value = mock_books

    actual = create_real_user_data_json_for_end_to_end_testing(mock_user_config)

    with open(mock_user_config.app_dir / "my_readwise_highlights.json") as file_handle:
        actual_content = json.load(file_handle)
        assert actual_content == mock_books
        assert actual is None
