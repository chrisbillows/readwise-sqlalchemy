from datetime import datetime
from typing import Any, Callable
from unittest.mock import ANY, MagicMock, Mock, patch

import pytest

from readwise_sqlalchemy.config import UserConfig
from readwise_sqlalchemy.main import (
    check_database,
    datetime_to_isoformat_str,
    fetch_books_with_highlights,
    fetch_from_export_api,
    main,
    run_pipeline,
    run_pipeline_flatten,
    update_database,
    validate_books_with_highlights,
)
from readwise_sqlalchemy.schemas import BookSchema
from tests.unit.test_schemas import mock_api_response


@pytest.fixture()
def mock_run_pipeline() -> tuple[dict, Any]:
    mocks = {
        "mock_setup_logging": MagicMock(),
        "mock_get_session": MagicMock(return_value="session"),
        "mock_check_database": MagicMock(return_value="last_fetch"),
        "mock_fetch_books_with_highlights": MagicMock(
            return_value=("data", "start", "end")
        ),
        "mock_validate_books_with_highlights": MagicMock(
            return_value=("valid_books", "invalid_books")
        ),
        "mock_update_database": MagicMock(),
    }
    actual = run_pipeline(
        user_config=MagicMock(DB="db"),
        setup_logging_func=mocks["mock_setup_logging"],
        get_session_func=mocks["mock_get_session"],
        check_db_func=mocks["mock_check_database"],
        fetch_func=mocks["mock_fetch_books_with_highlights"],
        validate_func=mocks["mock_validate_books_with_highlights"],
        update_db_func=mocks["mock_update_database"],
    )
    return mocks, actual


@pytest.fixture()
def mock_run_pipeline_flatten() -> tuple[dict, Any]:
    # Strings are used as simplified return values. They are not the real return types.
    mocks = {
        "mock_setup_logging": MagicMock(),
        "mock_get_session": MagicMock(return_value="session"),
        "mock_check_database": MagicMock(return_value="last_fetch"),
        "mock_fetch_books_with_highlights": MagicMock(
            return_value=("raw_data", "start", "end")
        ),
        "mock_flatten_books_with_highlights": MagicMock(
            return_value={
                "books": "book_data_dicts",
                "book_tags": "book_tags_dicts",
                "highlights": "highlights_dicts",
                "highlight_tags": "highlight_tags_dicts",
            }
        ),
        "mock_validate_flat_api_data_by_object": MagicMock(
            return_value=("valid_objs", "invalid_objs")
        ),
        "mock_update_database": MagicMock(),
    }
    actual = run_pipeline_flatten(
        user_config=MagicMock(DB="db"),
        setup_logging_func=mocks["mock_setup_logging"],
        get_session_func=mocks["mock_get_session"],
        check_db_func=mocks["mock_check_database"],
        fetch_func=mocks["mock_fetch_books_with_highlights"],
        flatten_func=mocks["mock_flatten_books_with_highlights"],
        validate_func=mocks["mock_validate_flat_api_data_by_object"],
        update_db_func=mocks["mock_update_database"],
    )
    return mocks, actual


@patch("readwise_sqlalchemy.main.requests")
def test_fetch_from_export_api(mock_requests: MagicMock):
    # Helper to build a mock response object
    def make_mock_response(json_data):
        mock_response = Mock()
        mock_response.json.return_value = json_data
        return mock_response

    # Set side_effect to return these responses on consecutive calls
    mock_requests.get.side_effect = [
        make_mock_response(
            {
                "count": 2,  # Page counter.
                "nextPageCursor": 98765432,  # Seem to be long ints.
                "results": [{"a": 1}, {"b": 2}],
            }
        ),
        make_mock_response(
            {
                "count": 1,
                "nextPageCursor": 87654321,
                "results": [{"c": 3}],
            }
        ),
        make_mock_response(
            {
                "count": 0,
                "nextPageCursor": None,
                "results": [],
            },
        ),
    ]
    last_fetch = "2025-04-14T20:28:21.589651"
    mock_user_config = Mock()
    mock_user_config.READWISE_API_TOKEN = "abc123"

    actual = fetch_from_export_api(last_fetch, mock_user_config)

    assert actual == [{"a": 1}, {"b": 2}, {"c": 3}]


@patch("readwise_sqlalchemy.main.create_database")
def test_check_database_when_database_doesnt_exist(
    mock_create_database: MagicMock, mock_user_config: UserConfig
):
    mock_session = Mock()
    actual = check_database(mock_session, mock_user_config)
    mock_create_database.assert_called_once_with(mock_user_config.db_path)
    assert actual is None


@patch("readwise_sqlalchemy.main.create_database")
@patch("readwise_sqlalchemy.main.get_last_fetch")
def test_check_database_when_database_exists(
    mock_query_last_fetch: MagicMock,
    mock_create_database: MagicMock,
):
    mock_session = MagicMock()

    mock_user_config = MagicMock()
    # Mock the database existing.
    mock_user_config.db_path.exists.return_value = True

    mock_last_fetch = datetime(2025, 1, 1, 1, 1, 1)
    mock_query_last_fetch.return_value = mock_last_fetch

    result = check_database(mock_session, mock_user_config)

    mock_user_config.db_path.exists.assert_called_once()
    mock_query_last_fetch.assert_called_once_with(mock_session)
    mock_create_database.assert_not_called()
    assert result == mock_last_fetch


def test_str_to_iso_format():
    date_time = datetime(2025, 4, 14, 20, 28, 21, 589651)
    expected = "2025-04-14T20:28:21.589651"
    actual = datetime_to_isoformat_str(date_time)
    assert actual == expected


@patch("readwise_sqlalchemy.main.datetime")
@patch("readwise_sqlalchemy.main.fetch_from_export_api")
@patch("readwise_sqlalchemy.main.datetime_to_isoformat_str")
def test_fetch_books_with_highlights_no_last_fetch(
    mock_str_to_iso_format: MagicMock,
    mock_fetch_from_export_api: MagicMock,
    mock_datetime: MagicMock,
):
    mock_api_response = ["book_with_hl"]
    mock_fetch_from_export_api.return_value = mock_api_response

    mock_start_new_fetch = datetime(2025, 1, 1, 1, 1, 1)
    mock_end_new_fetch = datetime(2025, 1, 1, 2, 2, 2)
    mock_datetime.now.side_effect = [mock_start_new_fetch, mock_end_new_fetch]

    last_fetch = None
    actual = fetch_books_with_highlights(last_fetch)

    mock_str_to_iso_format.assert_not_called()
    mock_datetime.now.assert_called()
    mock_fetch_from_export_api.assert_called_once_with(last_fetch)
    assert actual == (mock_api_response, mock_start_new_fetch, mock_end_new_fetch)


@patch("readwise_sqlalchemy.main.datetime")
@patch("readwise_sqlalchemy.main.fetch_from_export_api")
def test_fetch_books_with_highlights_last_fetch_exists(
    mock_fetch_from_export_api: MagicMock,
    mock_datetime: MagicMock,
):
    mock_api_response = ["book_with_hl"]
    mock_fetch_from_export_api.return_value = mock_api_response

    mock_start_new_fetch = datetime(2025, 1, 1, 2, 2, 2)
    mock_end_new_fetch = datetime(2025, 1, 1, 3, 3, 3)
    mock_datetime.now.side_effect = [mock_start_new_fetch, mock_end_new_fetch]

    last_fetch = datetime(2025, 1, 1, 1, 1, 1)
    last_fetch_iso_string = "2025-01-01T01:01:01"

    actual = fetch_books_with_highlights(last_fetch)

    mock_datetime.now.assert_called()
    mock_fetch_from_export_api.assert_called_once_with(last_fetch_iso_string)

    assert actual == (mock_api_response, mock_start_new_fetch, mock_end_new_fetch)


def test_validate_books_and_highlights_valid_book():
    mock_valid_book = mock_api_response()[0]

    mock_invalid_book = mock_api_response()[0]
    mock_invalid_book["user_book_id"] = "banana"

    mock_list_of_book_dicts = [mock_valid_book, mock_invalid_book]

    actual_valid, actual_failed = validate_books_with_highlights(
        mock_list_of_book_dicts
    )

    assert len(actual_valid) == 1
    assert len(actual_failed) == 1

    assert isinstance(actual_valid[0], BookSchema)
    assert getattr(actual_valid[0], "user_book_id") == mock_valid_book["user_book_id"]

    assert isinstance(actual_failed[0], tuple)
    failed_dict, failed_error = actual_failed[0]

    assert failed_dict["user_book_id"] == mock_invalid_book["user_book_id"]
    assert failed_error == (
        "1 validation error for BookSchema\nuser_book_id\n  Input should be a valid "
        "integer [type=int_type, input_value='banana', input_type=str]\n    "
        "For further information visit https://errors.pydantic.dev/2.11/v/int_type"
    )


@patch("readwise_sqlalchemy.main.DatabasePopulater")
def test_update_database(mock_db_populater: MagicMock):
    mock_instance = mock_db_populater.return_value

    update_database("session", "data", "start", "end")

    mock_db_populater.assert_called_once_with("session", "data", "start", "end")
    mock_instance.populate_database.assert_called_once_with()


@pytest.mark.parametrize(
    "mock_name, assertion",
    [
        ("mock_setup_logging", lambda m: m.assert_called_once_with()),
        ("mock_get_session", lambda m: m.assert_called_once()),
        # `Any` avoids passing in mock_user_config from mock_run_pipeline fixture.
        ("mock_check_database", lambda m: m.assert_called_once_with("session", ANY)),
        (
            "mock_fetch_books_with_highlights",
            lambda m: m.assert_called_once_with("last_fetch"),
        ),
        (
            "mock_validate_books_with_highlights",
            lambda m: m.assert_called_once_with("data"),
        ),
        (
            "mock_update_database",
            lambda m: m.assert_called_once_with(
                "session", "valid_books", "start", "end"
            ),
        ),
    ],
)
def test_run_pipeline_function_calls(
    mock_name: str,
    assertion: Callable,
    mock_run_pipeline: tuple[dict, Any],
):
    mocks, run_pipeline_return_value = mock_run_pipeline
    assertion(mocks[mock_name])


@pytest.mark.skip(reason="Revise once real functionality in place.")
@pytest.mark.parametrize(
    "mock_name, assertion",
    [
        ("mock_setup_logging", lambda m: m.assert_called_once_with()),
        ("mock_get_session", lambda m: m.assert_called_once()),
        # `Any` avoids passing in mock_user_config from mock_run_pipeline fixture.
        ("mock_check_database", lambda m: m.assert_called_once_with("session", ANY)),
        (
            "mock_fetch_books_with_highlights",
            lambda m: m.assert_called_once_with("last_fetch"),
        ),
        (
            "mock_flatten_books_with_highlights",
            lambda m: m.assert_called_once_with("raw_data"),
        ),
        # (
        #     "mock_validate_flat_api_data_by_object",
        #     lambda m: m.assert_has_calls(
        #         [
        #             call()
        #         ]
        #     ),
        # ),
        (
            "mock_update_database",
            lambda m: m.assert_called_once_with(
                "session", "valid_books", "start", "end"
            ),
        ),
    ],
)
def test_run_pipeline_flatten_function_calls(
    mock_name: str,
    assertion: Callable,
    mock_run_pipeline_flatten: tuple[dict, Any],
):
    mocks, run_pipeline_return_value = mock_run_pipeline_flatten
    assertion(mocks[mock_name])


def test_run_pipeline_return_value(mock_run_pipeline: tuple[dict, Any]):
    mocks, run_pipeline_return_value = mock_run_pipeline
    assert run_pipeline_return_value is None


@patch("readwise_sqlalchemy.main.run_pipeline")
def test_main(mock_run_pipeline: MagicMock):
    mock_user_config = Mock()
    main(mock_user_config)
    mock_run_pipeline.assert_called_once_with(mock_user_config)
