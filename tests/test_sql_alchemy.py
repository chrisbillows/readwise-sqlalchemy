import json
import sqlite3
from datetime import datetime

import pytest
from sqlalchemy.orm import Session

from readwise_sqlalchemy.sql_alchemy import (
    CommaSeparatedList,
    DatabasePopulater,
    JSONEncodedList,
    convert_iso_to_datetime,
    create_database,
    get_session,
)


def test_convert_to_datetime_valid():
    iso_date = "2025-01-01T00:00"
    actual = convert_iso_to_datetime(iso_date)
    expected = datetime(2025, 1, 1, 0, 0)
    assert actual == expected


def test_convert_to_datetime_invalid():
    not_an_iso_date = "not_a_date"
    with pytest.raises(ValueError):
        convert_iso_to_datetime(not_an_iso_date)


class TestJSONEncodedList:
    """Test JSONEncodedList decorator."""

    JSON_LIST_OF_DICTS = [{"hl_location1": "hl_name1"}, {"hl_location2": "hl_name2"}]
    STRING = '[{"hl_location1": "hl_name1"}, {"hl_location2": "hl_name2"}]'

    def test_process_bind_param_valid(self):
        """Test the JSON encoding/binding."""
        decorator = JSONEncodedList()
        actual = decorator.process_bind_param(self.JSON_LIST_OF_DICTS, None)
        assert actual == self.STRING

    def test_process_result_value_valid(self):
        """Test the JSON decoding/result."""
        decorator = JSONEncodedList()
        decoded = decorator.process_result_value(self.STRING, None)
        assert decoded == self.JSON_LIST_OF_DICTS


class TestCommaSeparatedList:
    """Test CommaSeparatedList decorator."""

    JSON_LIST_OF_STRINGS = ["book_tag_1", "book_tag_2"]
    STRING = "book_tag_1,book_tag_2"

    def test_process_bind_param_valid(self):
        """Test the CSL encoding/binding."""
        decorator = CommaSeparatedList()
        actual = decorator.process_bind_param(self.JSON_LIST_OF_STRINGS, None)
        assert actual == self.STRING

    def test_process_result_value_valid(self):
        """Test the CSL decoding/result."""
        decorator = CommaSeparatedList()
        decoded = decorator.process_result_value(self.STRING, None)
        assert decoded == self.JSON_LIST_OF_STRINGS


def test_create_database_tables(synthetic_user_config):
    """Test a database is created with the correctly named tables."""
    create_database(synthetic_user_config.DB)
    expected = [("books",), ("readwise_batches",), ("highlights",)]

    connection = sqlite3.connect(synthetic_user_config.DB)
    cursor = connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    actual = cursor.fetchall()

    connection.close()

    assert actual == expected


def test_get_session(synthetic_user_config):
    """Test a Session object is returned (which is assumed to be valid)."""
    actual = get_session(synthetic_user_config.DB)
    assert isinstance(actual, Session)


class TestDatabasePopulater:
    @pytest.fixture(autouse=True)
    def test_setup(self, synthetic_user_config):
        # Attach the synthetic user_config to the object
        self.user_config = synthetic_user_config
        # Create a temporary database
        create_database(self.user_config.DB)
        session = get_session(self.user_config)
        sample_API_data = "tests/data/real/sample_updated_books.json"
        with open(sample_API_data, "r") as file_handle:
            books_and_highlights = json.load(file_handle)
        start_fetch = datetime(2025, 1, 1, 1, 0)
        end_fetch = datetime(2025, 1, 1, 1, 1)
        dbp = DatabasePopulater(session, books_and_highlights, start_fetch, end_fetch)
        self.dbp = dbp

    def test_init(self):
        """Basic test that an object can be instantiated with expected values."""
        assert list(self.dbp.__dict__.keys()) == [
            "session",
            "books",
            "start_fetch",
            "end_fetch",
        ]

    def test_process_batch(self):
        pass

    def test_process_book(self):
        pass

    def test_validate_book_id(self):
        pass

    def test_validate_highlight_id(self):
        pass

    def test_process_highlight(self):
        pass
