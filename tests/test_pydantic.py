from typing import Any

import pytest
from pydantic import ValidationError

from .conftest import HIGHLIGHT_SCHEMA_VARIANTS, BOOK_SCHEMA_VARIANTS
from readwise_sqlalchemy.pydantic import BookSchema, HighlightSchema


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
