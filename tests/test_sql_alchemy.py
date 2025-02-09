import json
import sqlite3
from datetime import datetime, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from readwise_sqlalchemy.sql_alchemy import (
    Book,
    CommaSeparatedList,
    DatabasePopulater,
    Highlight,
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
    """Test the Session database url has the correct file name."""
    session = get_session(synthetic_user_config.DB)
    database_url: str = session.bind.url
    actual = str(database_url).split("/")[-1]
    assert actual == "readwise.db"


# TODO: The tests current use sampled real data. This needs to be anonymised and the and
# TODO: the tests revised to match.
class TestDatabasePopulater:
    """
    Test the Database Populater class.

    The database is created with SQLite to isolate the DatabasePopulater logic. SQLite
    is used in tests where appropriate to further isolate the SQL Alchemy logic.
    """

    @pytest.fixture(autouse=True)
    def test_setup(self, synthetic_user_config):
        """Instantiate an object and make available to all tests in the test class.

        Attributes
        ----------
        user_config: UserConfig
            A valid UserConfig object, with the path to a database created using
            sqlite3.
        dbp: DatabaseProcessor
            An instantiated DatabaseProcess object.
        """
        self.user_config = synthetic_user_config

        create_database(self.user_config.DB)
        connection = sqlite3.connect(self.user_config.DB)
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        session = get_session(self.user_config.DB)
        sample_API_data = "tests/data/real/sample_updated_books.json"
        with open(sample_API_data, "r") as file_handle:
            books_and_highlights = json.load(file_handle)

        start_fetch = datetime(2025, 1, 1, 1, 0)
        end_fetch = datetime(2025, 1, 1, 1, 1)

        self.dbp = DatabasePopulater(
            session, books_and_highlights, start_fetch, end_fetch
        )

    def test_init(self):
        """Test the object has been instantiated with the expected attributes."""
        assert list(self.dbp.__dict__.keys()) == [
            "session",
            "books",
            "start_fetch",
            "end_fetch",
        ]

    @pytest.mark.parametrize(
        "slice, expected",
        [
            (0, 1),
            (1, "2025-01-01 01:00:00.000000"),
            (2, "2025-01-01 01:01:00.000000"),
        ],
    )
    def test_populate_database_successfully_committed_readwise_batch(
        self, slice, expected
    ):
        """Test populate database successfully commits the readwise batch.

        Test sample values from the first (and only) expected entry in the table.
        """
        self.dbp.populate_database()
        connection = sqlite3.connect(self.user_config.DB)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM readwise_batches")
        actual = cursor.fetchall()[0]
        assert actual[slice] == expected

    @pytest.mark.parametrize(
        "slice, expected",
        [
            (0, 813795626),
            (1, 45978496),
            (15, "https://readwise.io/open/813795626"),
        ],
    )
    def test_populate_database_successfully_committed_highlights(self, slice, expected):
        """Test populate database successfully commits highlights.

        Test sample values from the first entry in the table.
        """
        self.dbp.populate_database()
        connection = sqlite3.connect(self.user_config.DB)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM highlights")
        actual = cursor.fetchall()[0]
        assert actual[slice] == expected

    @pytest.mark.parametrize(
        "slice, expected",
        [
            (0, 7994381),
            (1, "Tweets From Elon Musk"),
            (
                5,
                "https://pbs.twimg.com/profile_images/1858316737780781056/kPL61o0F.jpg",
            ),
        ],
    )
    def test_populate_database_successfully_committed_books(self, slice, expected):
        """Test populate database successfully commits highlights.

        Uses a sqlite3 query to test the first entry in the highlights table contains
        the expected values at sample indexes

        """
        self.dbp.populate_database()
        connection = sqlite3.connect(self.user_config.DB)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM books")
        actual = cursor.fetchall()[0]
        print(actual)
        assert actual[slice] == expected

    def test_process_batch(self):
        """Test adds a single ReadwiseBatch with the correct info to the session."""
        self.dbp._process_batch()
        stmt = select(ReadwiseBatch)
        results = self.dbp.session.execute(stmt).scalars().all()
        first_result = results[0]
        actual = vars(first_result)
        expected = {
            "batch_id": 1,
            "start_time": datetime(2025, 1, 1, 1, 0),
            "end_time": datetime(2025, 1, 1, 1, 1),
        }
        assert expected.items() <= actual.items()

    def test_process_book(self):
        """Test adding a single book is to the session."""
        book = self.dbp.books[0]
        book.pop("highlights", [])
        self.dbp._process_book(book)
        stmt = select(Book)
        results = self.dbp.session.execute(stmt).scalars().all()
        actual = vars(results[0])
        expected = {"user_book_id": 46095532, "author": "Hugo Rifkind"}
        assert expected.items() <= actual.items()

    def test_validate_a_highlights_book_id_valid(self):
        """Validate a valid book id."""
        book = self.dbp.books[0]
        highlights = book.pop("highlights", [])
        book_obj = Book(**book)
        assert self.dbp._validate_book_id(highlights[0], book_obj) is None

    def test_validate_a_highlights_book_id_invalid(self):
        """Attempts to validate a book id"""
        book = self.dbp.books[0]
        book["user_book_id"] = 123
        highlights = book.pop("highlights", [])
        book_obj = Book(**book)
        with pytest.raises(ValueError):
            self.dbp._validate_book_id(highlights[0], book_obj)

    def test_validate_highlight_id_valid(self):
        """Validate a valid highlight id."""
        book = self.dbp.books[0]
        highlight = book["highlights"][0]
        book.pop("highlights")
        book_obj = Book(**book)
        assert self.dbp._validate_highlight_id(highlight, book_obj) is None

    def test_validate_highlight_id_invalid(self):
        """Attempts to validate a highlight already added to the session."""
        book = self.dbp.books[0]
        highlights = book.pop("highlights", [])
        highlight = highlights[0]
        book_obj = Book(**book)
        # Coupled test to another method for convenience.
        self.dbp._process_highlight(highlight, book_obj)
        with pytest.raises(ValueError):
            self.dbp._validate_highlight_id(highlight, book_obj)

    def test_process_highlight(self):
        """Test adding a single highlight to the session."""
        book = self.dbp.books[0]
        highlights = book.pop("highlights", [])
        highlight = highlights[0]
        book_obj = Book(**book)
        self.dbp._process_highlight(highlight, book_obj)
        stmt = select(Highlight)
        results = self.dbp.session.execute(stmt).scalars().all()
        assert len(results) == 1
        actual = vars(results[0])
        expected = {
            "id": 815092566,
            "location": 38975,
            "location_type": "offset",
            "note": "",
            "color": "",
            "highlighted_at": datetime(
                2024, 11, 21, 14, 7, 59, 74000, tzinfo=timezone.utc
            ),
            "created_at": datetime(
                2024, 11, 21, 14, 7, 59, 102000, tzinfo=timezone.utc
            ),
            "updated_at": datetime(
                2024, 11, 21, 14, 7, 59, 102000, tzinfo=timezone.utc
            ),
            "end_location": None,
            "tags": [],
            "is_favorite": False,
            "is_discard": False,
            "book_id": 46095532,
        }
        assert expected.items() <= actual.items()
