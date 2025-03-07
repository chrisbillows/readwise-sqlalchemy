from typing import Any

import pytest
from pydantic import ValidationError

from readwise_sqlalchemy.sql_alchemy import Book
from readwise_sqlalchemy.pydantic import BookSchema, HighlightSchema, HighlightTagsSchema

from .conftest import (
    BOOK_SCHEMA_VARIANTS, HIGHLIGHT_SCHEMA_VARIANTS, HIGHLIGHT_TAGS_SCHEMA_VARIANTS
)


def test_highlight_tags_schema_with_valid_values(mock_highlight_tags: dict):
    assert HighlightTagsSchema(**mock_highlight_tags)


@pytest.mark.parametrize("invalid_field", HIGHLIGHT_TAGS_SCHEMA_VARIANTS.keys())
def test_highlight_tags_schema_with_invalid_types(
    invalid_field: str, mock_highlight_tags: dict
): 
    mock_highlight_tags[invalid_field] = HIGHLIGHT_TAGS_SCHEMA_VARIANTS[invalid_field][
        "value_invalid_type"
    ]
    with pytest.raises(ValidationError):
        HighlightSchema(**mock_highlight_tags)    


@pytest.mark.parametrize(
    "valid_null_field",
    [
        field
        for field in HIGHLIGHT_TAGS_SCHEMA_VARIANTS.keys()
        if HIGHLIGHT_TAGS_SCHEMA_VARIANTS[field]["nullable"]
    ],
)
def test_highlight_tags_schema_with_null_values_where_allowed(
    valid_null_field: str, mock_highlight_tags: dict
):
    mock_highlight_tags[valid_null_field] = None
    assert HighlightTagsSchema(**mock_highlight_tags)


# Skips as currently no invalid null fields.
@pytest.mark.parametrize(
    "invalid_null_field",
    [
        field
        for field in HIGHLIGHT_TAGS_SCHEMA_VARIANTS.keys()
        if not HIGHLIGHT_TAGS_SCHEMA_VARIANTS[field]["nullable"]
    ],
)
def test_highlight_tags_schema_with_null_values_where_not_allowed(
    invalid_null_field: str, mock_highlight_tags: dict
):
    mock_highlight_tags[invalid_null_field] = None
    with pytest.raises(ValidationError):
        HighlightTagsSchema(**mock_highlight_tags)


# Raise if any field is missing.
@pytest.mark.parametrize(
    "removed_field", [field for field in HIGHLIGHT_TAGS_SCHEMA_VARIANTS.keys()]
)
def test_highlight_tags_schema_with_missing_fields(
    removed_field: str, mock_highlight_tags: dict[str, Any]
):
    del mock_highlight_tags[removed_field]
    # Missing fields are intended to be allowed.
    HighlightTagsSchema(**mock_highlight_tags)


def test_highlight_tags_schema_config_with_unexpected_field(mock_highlight_tags: dict):
    mock_highlight_tags['extra_field'] = None
    with pytest.raises(ValidationError):
        HighlightTagsSchema(**mock_highlight_tags)


def test_highlight_schema_with_valid_values(mock_highlight: dict):
    assert HighlightSchema(**mock_highlight)
       

@pytest.mark.parametrize("invalid_field", HIGHLIGHT_SCHEMA_VARIANTS.keys())
def test_highlight_schema_with_invalid_types(
    invalid_field: str, mock_highlight: dict[str, Any]
):
    mock_highlight[invalid_field] = HIGHLIGHT_SCHEMA_VARIANTS[invalid_field][
        "value_invalid_type"
    ]
    if invalid_field == 'tags':
        breakpoint()
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


def test_highlight_schema_config_with_unexpected_field(mock_highlight:dict):
    mock_highlight['extra_field'] = None
    with pytest.raises(ValidationError):
        HighlightSchema(**mock_highlight)


def test_highlight_replace_null_with_empty_list_for_tags(mock_highlight: dict):
    mock_highlight['tags'] = None
    highlight = HighlightSchema(**mock_highlight)
    assert highlight.tags == []


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

        
def test_book_schema_config_with_unexpected_field(mock_book: dict):
    mock_book['extra_field'] = None
    with pytest.raises(ValidationError):
        HighlightSchema(**mock_book)


def test_book_replace_null_with_empty_list_for_book_tags(mock_book: dict):
    mock_book['book_tags'] = None
    book = BookSchema(**mock_book)
    assert book.book_tags == []



