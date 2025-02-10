import pytest
from pydantic import ValidationError

from readwise_sqlalchemy.pydantic import BookSchema

# Construct various test arrays. Invalid types are just a single example, they are not
# exhaustive.
SCHEMA_VARIATIONS = {
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
}


@pytest.fixture
def mock_book():
    """Return a valid mock book."""
    fields = SCHEMA_VARIATIONS.keys()
    return {field: SCHEMA_VARIATIONS[field]["value_valid"] for field in fields}


def test_book_schema_with_valid_values(mock_book):
    book = BookSchema(**mock_book)
    assert book


@pytest.mark.parametrize("invalid_field", SCHEMA_VARIATIONS.keys())
def test_book_schema_with_invalid_types(invalid_field, mock_book):
    mock_book[invalid_field] = SCHEMA_VARIATIONS[invalid_field]["value_invalid_type"]
    print(mock_book)
    with pytest.raises(ValidationError):
        BookSchema(**mock_book)


@pytest.mark.parametrize(
    "valid_null_field",
    [
        field
        for field in SCHEMA_VARIATIONS.keys()
        if SCHEMA_VARIATIONS[field]["nullable"]
    ],
)
def test_book_schema_with_null_values_where_allowed(valid_null_field, mock_book):
    mock_book[valid_null_field] = None
    book = BookSchema(**mock_book)
    assert book


@pytest.mark.parametrize(
    "invalid_null_field",
    [
        field
        for field in SCHEMA_VARIATIONS.keys()
        if not SCHEMA_VARIATIONS[field]["nullable"]
    ],
)
def test_book_schema_with_null_values_where_not_allowed(invalid_null_field, mock_book):
    mock_book[invalid_null_field] = None
    with pytest.raises(ValidationError):
        BookSchema(**mock_book)


# Error if any field is missing.
@pytest.mark.parametrize("removed_field", [field for field in SCHEMA_VARIATIONS.keys()])
def test_book_schema_with_missing_fields(removed_field, mock_book):
    del mock_book[removed_field]
    # if removed_field == "cover_image_url":
    #     breakpoint()
    with pytest.raises(ValidationError):
        BookSchema(**mock_book)
