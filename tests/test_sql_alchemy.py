import json
import sqlite3
from datetime import datetime

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from readwise_sqlalchemy.sql_alchemy import (
    CommaSeparatedList,
    DatabasePopulater,
    JSONEncodedList,
    ReadwiseBatch,
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


def test_get_session_database_url(synthetic_user_config):
    """"""
    session = get_session(synthetic_user_config.DB)
    database_url: str = session.bind.url
    actual = str(database_url).split("/")[-1]
    assert actual == "readwise.db"


class TestDatabasePopulater:
    @pytest.fixture(autouse=True)
    def test_setup(self, synthetic_user_config):
        print(2.1, synthetic_user_config.APPLICATION_DIR)
        print(2.2, synthetic_user_config.ENV_FILE)
        print(2.3, synthetic_user_config.READWISE_API_TOKEN)
        print(2.4, synthetic_user_config.DB)

        # Attach the synthetic user_config to the object
        self.user_config = synthetic_user_config
        print(3.1, self.user_config.APPLICATION_DIR)
        print(3.2, self.user_config.ENV_FILE)
        print(3.3, self.user_config.READWISE_API_TOKEN)
        print(3.4, self.user_config.DB)

        # Create a temporary database
        create_database(self.user_config.DB)
        connection = sqlite3.connect(self.user_config.DB)
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        print(4, cursor.fetchall())

        session = get_session(self.user_config.DB)
        print(5.1, type(session))
        print(5.2, session.bind.url)

        sample_API_data = "tests/data/real/sample_updated_books.json"
        with open(sample_API_data, "r") as file_handle:
            books_and_highlights = json.load(file_handle)
        start_fetch = datetime(2025, 1, 1, 1, 0)
        end_fetch = datetime(2025, 1, 1, 1, 1)
        dbp = DatabasePopulater(session, books_and_highlights, start_fetch, end_fetch)
        self.dbp = dbp

    # def test_init(self):
    #     """Basic test that an object can be instantiated with expected values."""
    #     assert list(self.dbp.__dict__.keys()) == [
    #         "session",
    #         "books",
    #         "start_fetch",
    #         "end_fetch",
    #     ]

    def test_process_batch(self):
        """Test adds a single ReadwiseBatch with the correct info to the session."""
        # print(7.1, self.user_config.APPLICATION_DIR)
        # print(7.2, self.user_config.ENV_FILE)
        # print(7.3, self.user_config.READWISE_API_TOKEN)
        # print(7.4, self.user_config.DB)
        # connection = sqlite3.connect(self.user_config.DB)
        # cursor = connection.cursor()
        # cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        # print(7.5, cursor.fetchall())
        self.dbp._process_batch()
        # inspector = inspect(self.dbp.session.get_bind())
        # print("8 Existing tables:", inspector.get_table_names())
        stmt = select(ReadwiseBatch)
        results = self.dbp.session.execute(stmt).scalars().all()
        first_result = results[0]
        # print("batch_id", first_result.batch_id)
        # print("start", first_result.start_time)
        # print("end", first_result.end_time)
        # print("write", first_result.database_write_time)
        # print(8.1, dir(self.user_config))
        # print(8.2, "\n", "".join((f"{k}{'\n'}{'\t'}{v}{'\n'}" for k, v in self.dbp.session.bind.__dict__.items())))
        actual = vars(first_result)
        expected = {
            "batch_id": 1,
            "start_time": datetime(2025, 1, 1, 1, 0),
            "end_time": datetime(2025, 1, 1, 1, 1),
        }
        assert expected.items() <= actual.items()

    def test_process_book(self):
        pass

    # def test_validate_book_id(self):
    #     pass

    # def test_validate_highlight_id(self):
    #     pass

    # def test_process_highlight(self):
    #     pass
