import sqlite3
from datetime import datetime

import pytest
from sqlalchemy.orm import Session

from readwise_sqlalchemy.sql_alchemy import (
    CommaSeparatedList,
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
