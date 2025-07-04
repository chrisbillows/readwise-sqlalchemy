import json
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from readwise_local_plus.config import UserConfig
from readwise_local_plus.models import (
    Base,
    Book,
    BookTag,
    Highlight,
    HighlightTag,
    ReadwiseBatch,
)
from readwise_local_plus.utils import (
    fetch_real_user_data_json_for_end_to_end_testing,
    get_columns_and_values,
    list_invalid_db_objects,
    log_to_stdout_readwise_api_fetch_since_custom_date,
    readwise_api_fetch_since_custom_date,
    write_to_json_readwise_api_fetch_since_custom_date,
)
from tests.helpers import flat_mock_api_response_fully_validated, mock_api_response


@pytest.mark.skip(reason="Requires a database fixture e.g. in conftest.py")
def test_get_columns_and_values(minimal_book_as_orm):
    actual = get_columns_and_values(minimal_book_as_orm)
    expected = {
        "asin": None,
        "author": None,
        "batch_id": 1,
        "category": None,
        "cover_image_url": None,
        "document_note": None,
        "external_id": None,
        "is_deleted": None,
        "user_book_id": 99,
        "readable_title": None,
        "readwise_url": None,
        "source": None,
        "source_url": None,
        "summary": None,
        "title": "book_2",
        "unique_url": None,
        "validated": True,
        "validation_errors": {},
    }
    assert actual == expected


@patch("readwise_local_plus.utils.fetch_from_export_api")
def test_fetch_real_user_data_json_for_end_to_end_testing(
    mock_fetch_from_export_api: MagicMock, mock_user_config: UserConfig
):
    mock_books = ["book_1", "book_2"]
    mock_fetch_from_export_api.return_value = mock_books

    actual = fetch_real_user_data_json_for_end_to_end_testing(mock_user_config)

    with open(mock_user_config.app_dir / "my_readwise_highlights.json") as file_handle:
        actual_content = json.load(file_handle)
        assert actual_content == mock_books
        assert actual is None


@patch("readwise_local_plus.utils.FileHandler.write_json")
def test_write_to_json_readwise_api_fetch_since_custom_date(
    mock_write_json, mock_user_config
):
    books = mock_api_response()
    updates_since = "2025-01-01T00:00:00"

    write_to_json_readwise_api_fetch_since_custom_date(
        books, updates_since, mock_user_config
    )

    # Assert: Directory created
    expected_dir = mock_user_config.app_dir / "readwise_custom_fetches"
    assert expected_dir.exists() and expected_dir.is_dir()

    # Assert: README.md created
    readme = expected_dir / "README.md"
    assert readme.exists()
    readme_content = readme.read_text()
    assert "User created custom fetches" in readme_content

    # Assert: FileHandler.write_json called with correct file path
    expected_file = expected_dir / f"custom_rw_fetch_{updates_since}.json"
    mock_write_json.assert_called_once_with(books, expected_file)


def test_log_to_stdout_readwise_api_fetch_since_custom_date(caplog):
    books = mock_api_response()
    updates_since = "2025-01-01T00:00:00"

    with caplog.at_level("INFO"):
        log_to_stdout_readwise_api_fetch_since_custom_date(books, updates_since)

    logs = caplog.text
    assert f"{len(books)} books updated since {updates_since}" in logs

    book = books[0]
    highlight = book["highlights"][0]

    assert f"book: {book['readable_title']}" in logs
    assert f"h/lights: {len(book['highlights'])}" in logs
    assert f"cat: {book['category']}" in logs
    assert f"author: {book['author']}" in logs
    assert f"source: {book['source']}" in logs
    assert highlight["text"][:80] in logs
    assert f"{highlight.get('highlighted_at')}" in logs
    assert f"{highlight.get('created_at')}" in logs
    assert f"{highlight.get('updated_at')}" in logs


@patch("readwise_local_plus.utils.fetch_from_export_api")
@patch("readwise_local_plus.utils.fetch_user_config")
@patch("readwise_local_plus.utils.log_to_stdout_readwise_api_fetch_since_custom_date")
@patch("readwise_local_plus.utils.write_to_json_readwise_api_fetch_since_custom_date")
def test_readwise_api_fetch_since_custom_date(
    mock_write_json,
    mock_log_stdout,
    mock_fetch_user_config,
    mock_fetch_api,
):
    mock_user_config = MagicMock(spec=UserConfig)
    mock_fetch_user_config.return_value = mock_user_config
    mock_fetch_api.return_value = mock_api_response()

    actual = readwise_api_fetch_since_custom_date("2024-07-01T00:00:00", log=True)

    mock_fetch_user_config.assert_called_once()
    mock_fetch_api.assert_called_once_with(
        last_fetch="2024-07-01T00:00:00", user_config=mock_user_config
    )
    mock_log_stdout.assert_called_once_with(mock_api_response(), "2024-07-01T00:00:00")
    mock_write_json.assert_called_once_with(
        mock_api_response(), "2024-07-01T00:00:00", mock_user_config
    )
    assert actual == mock_api_response()


def test_list_invalid_db_objects(capsys: pytest.CaptureFixture[str]):
    orm_models = [Book, BookTag, Highlight, HighlightTag]
    test_objects = flat_mock_api_response_fully_validated()
    test_objects = [obj[0] for obj in test_objects.values()]
    for obj in test_objects:
        obj["validated"] = False
        obj["validation_errors"].update({"mock_field": "mock_error"})

    ANY_TIME = datetime(2025, 1, 1, 10, 10, 10)
    batch = ReadwiseBatch(start_time=ANY_TIME, end_time=ANY_TIME)
    orm_models = [
        model(**obj, batch=batch) for model, obj in zip(orm_models, test_objects)
    ]

    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test.db"
        engine = create_engine(f"sqlite:///{db_path}")
        Session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)

        with Session() as session:
            session.add(batch)
            session.flush()
            session.add_all(orm_models)
            batch.database_write_time = ANY_TIME
            session.commit()

            mock_user_config = Mock()
            mock_user_config.db_path = db_path

            list_invalid_db_objects(mock_user_config)

            captured = capsys.readouterr()
            actual = captured.out

            # These are the actual instances but we're only using their string
            # representation.
            expected = (
                f"4 invalid objects found:\n"
                f"[Book] {orm_models[0]}\n"
                f"  - mock_field: mock_error\n"
                f"[BookTag] {orm_models[1]}\n"
                f"  - mock_field: mock_error\n"
                f"[Highlight] {orm_models[2]}\n"
                f"  - mock_field: mock_error\n"
                f"[HighlightTag] {orm_models[3]}\n"
                f"  - mock_field: mock_error\n"
            )
            assert actual == expected
