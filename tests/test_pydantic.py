from typing import Any

import pytest
from pydantic import ValidationError

from readwise_sqlalchemy.pydantic import BookSchema, HighlightSchema

# Construct various test dicts. 'value_invalid_type' is a single example of an invalid
# type. It is not exhaustive.
HIGHLIGHT_SCHEMA_VARIANTS = {
    "id": {
        "value_valid": 1,
        "value_invalid_type": "string",
        "nullable": False,
    },
    "text": {
        "value_valid": "I am the text of a highlight",
        "value_invalid_type": 987654321,
        "nullable": False,
    },
    "location": {
        "value_valid": 12345,
        "value_invalid_type": "one two three",
        "nullable": True,
    },
    "location_type": {
        "value_valid": "page",
        "value_invalid_type": "time",
        "nullable": True,
    },
    "note": {
        "value_valid": "",
        "value_invalid_type": [],
        "nullable": True,
    },
    "color": {
        "value_valid": "yellow",
        "value_invalid_type": "magenta",
        "nullable": True,
    },
    "highlighted_at": {
        "value_valid": "2025-01-01T01:02:04.456Z",
        "value_invalid_type": "2024-13-01T10:20:30.123Z",
        "nullable": True,
    },
    "created_at": {
        "value_valid": "2025-01-01T01:02:04.456Z",
        "value_invalid_type": "2024-13-01T10:20:30.123Z",
        "nullable": True,
    },
    "updated_at": {
        "value_valid": "2025-01-01T01:02:04.456Z",
        "value_invalid_type": "2024-13-01T10:20:30.123Z",
        "nullable": True,
    },
    "external_id": {
        "value_valid": "6320b2bd7fbcdd7b0c000b3e",
        "value_invalid_type": 12345,
        "nullable": True,
    },
    "end_location": {
        "value_valid": None,
        "value_invalid_type": 12345,
        "nullable": True,
    },
    "url": {
        "value_valid": "http://www.url.com",
        "value_invalid_type": "not-a-url",
        "nullable": True,
    },
    "book_id": {
        "value_valid": 1,
        "value_invalid_type": -1,
        "nullable": False,
    },
    "tags": {  # The 'HighlightTagsSchema' is simple enough to test here.
        "value_valid": [{"id": 1234, "name": "tag_2"}],
        "value_invalid_type": [{"id": 1234, "name": 1234}],
        "nullable": True,
    },
    "is_favorite": {
        "value_valid": True,
        "value_invalid_type": "true",
        "nullable": True,
    },
    "is_discard": {
        "value_valid": False,
        "value_invalid_type": "false",
        "nullable": True,
    },
    "readwise_url": {
        "value_valid": "https://readwise.io/open/123456",
        "value_invalid_type": "readwise.io/open/123456",
        "nullable": True,
    },
}

# Construct various test dicts. 'value_invalid_type' is a single example of an invalid
# type. It is not exhaustive.
BOOK_SCHEMA_VARIANTS = {
    "user_book_id": {
        "value_valid": 1,
        "value_invalid_type": "string",
        "nullable": False,
    },
    "title": {
        "value_valid": "Example Book Title",
        "value_invalid_type": 100,
        "nullable": False,
    },
    "author": {
        "value_valid": "Arthur Author",
        "value_invalid_type": [],
        "nullable": True,
    },
    "readable_title": {
        "value_valid": "Example Book Title",
        "value_invalid_type": {},
        "nullable": False,
    },
    "source": {
        "value_valid": "sauce",
        "value_invalid_type": "a",
        "nullable": True,
    },
    "cover_image_url": {
        "value_valid": "http://www.image.com/image.jpg",
        "value_invalid_type": "not_a_web_address",
        "nullable": True,
    },
    "unique_url": {
        "value_valid": "http://www.article.com/article",
        "value_invalid_type": (),
        "nullable": True,
    },
    "summary": {
        "value_valid": "An example summary",
        "value_invalid_type": 987,
        "nullable": True,
    },
    "book_tags": {
        "value_valid": [],
        "value_invalid_type": [1, 2, 3],
        "nullable": False,
    },
    "category": {
        "value_valid": "books",
        "value_invalid_type": "youtube",
        "nullable": False,
    },
    "readwise_url": {
        "value_valid": "http://www.readwise.io/book123",
        "value_invalid_type": "a_normal_string",
        "nullable": False,
    },
    "source_url": {
        "value_valid": "http://www.source.com/the_source",
        "value_invalid_type": "a_normal_string",
        "nullable": True,
    },
    "asin": {
        "value_valid": "A0099BC1Z0",
        "value_invalid_type": "A00-BC-1Z0",
        "nullable": True,
    },
    "highlights": {
        "value_valid": [],
        "value_invalid_type": 123,
        "nullable": False,
    },
}


@pytest.fixture
def mock_highlight():
    fields = HIGHLIGHT_SCHEMA_VARIANTS.keys()
    return {field: HIGHLIGHT_SCHEMA_VARIANTS[field]["value_valid"] for field in fields}


def test_highlight_schema_with_valid_values(mock_highlight: dict):
    assert HighlightSchema(**mock_highlight)


@pytest.mark.parametrize("invalid_field", HIGHLIGHT_SCHEMA_VARIANTS.keys())
def test_highlight_schema_with_invalid_types(
    invalid_field: str, mock_highlight: dict[str, Any]
):
    mock_highlight[invalid_field] = HIGHLIGHT_SCHEMA_VARIANTS[invalid_field][
        "value_invalid_type"
    ]
    with pytest.raises(ValidationError):
        HighlightSchema(**mock_highlight)


@pytest.mark.parametrize(
    "valid_null_field",
    [
        field
        for field in HIGHLIGHT_SCHEMA_VARIANTS.keys()
        if HIGHLIGHT_SCHEMA_VARIANTS[field]["nullable"]
    ],
)
def test_highlight_schema_with_null_values_where_allowed(
    valid_null_field: str, mock_highlight: dict[str, Any]
):
    mock_highlight[valid_null_field] = None
    assert HighlightSchema(**mock_highlight)


@pytest.mark.parametrize(
    "invalid_null_field",
    [
        field
        for field in HIGHLIGHT_SCHEMA_VARIANTS.keys()
        if not HIGHLIGHT_SCHEMA_VARIANTS[field]["nullable"]
    ],
)
def test_highlight_schema_with_null_values_where_not_allowed(
    invalid_null_field: str, mock_highlight: dict[str, Any]
):
    mock_highlight[invalid_null_field] = None
    with pytest.raises(ValidationError):
        HighlightSchema(**mock_highlight)


# Raise if any field is missing.
@pytest.mark.parametrize(
    "removed_field", [field for field in HIGHLIGHT_SCHEMA_VARIANTS.keys()]
)
def test_highlight_schema_with_missing_fields(
    removed_field: str, mock_highlight: dict[str, Any]
):
    del mock_highlight[removed_field]
    with pytest.raises(ValidationError):
        HighlightSchema(**mock_highlight)


@pytest.fixture
def mock_book():
    """Return a valid mock book."""
    fields = BOOK_SCHEMA_VARIANTS.keys()
    return {field: BOOK_SCHEMA_VARIANTS[field]["value_valid"] for field in fields}


def test_book_schema_with_valid_values(mock_book: dict):
    assert BookSchema(**mock_book)


@pytest.mark.parametrize("invalid_field", BOOK_SCHEMA_VARIANTS.keys())
def test_book_schema_with_invalid_types(invalid_field: str, mock_book: dict[str, Any]):
    mock_book[invalid_field] = BOOK_SCHEMA_VARIANTS[invalid_field]["value_invalid_type"]
    with pytest.raises(ValidationError):
        BookSchema(**mock_book)


@pytest.mark.parametrize(
    "valid_null_field",
    [
        field
        for field in BOOK_SCHEMA_VARIANTS.keys()
        if BOOK_SCHEMA_VARIANTS[field]["nullable"]
    ],
)
def test_book_schema_with_null_values_where_allowed(
    valid_null_field: str, mock_book: dict[str, Any]
):
    mock_book[valid_null_field] = None
    assert BookSchema(**mock_book)


@pytest.mark.parametrize(
    "invalid_null_field",
    [
        field
        for field in BOOK_SCHEMA_VARIANTS.keys()
        if not BOOK_SCHEMA_VARIANTS[field]["nullable"]
    ],
)
def test_book_schema_with_null_values_where_not_allowed(
    invalid_null_field: str, mock_book: dict[str, Any]
):
    mock_book[invalid_null_field] = None
    with pytest.raises(ValidationError):
        BookSchema(**mock_book)


# Raise if any field is missing.
@pytest.mark.parametrize(
    "removed_field", [field for field in BOOK_SCHEMA_VARIANTS.keys()]
)
def test_book_schema_with_missing_fields(removed_field: str, mock_book: dict[str, Any]):
    del mock_book[removed_field]
    with pytest.raises(ValidationError):
        BookSchema(**mock_book)
