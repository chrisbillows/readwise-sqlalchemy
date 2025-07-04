"""
Test helpers used across multiple test modules.

This module mirrors the main stages of the Readwise API data processing pipeline.
Data moves from:

        -> mock_api_response
            -> mock_api_response_nested_validated
                -> flat_mock_api_response_nested_validated
                    -> flat_mock_api_response_fully_validated

to the database, with each stage adding validation and flattening the data.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from readwise_local_plus.types import FetchFn, FlattenFn, ValidateNestedObjFn


@dataclass
class DbHandle:
    """
    Group SQL Alchemy database connection objects.

    Attributes
    ----------
    engine: Engine
        Engine bound to a database.
    session: Session
        Session bound to an engine.
    """

    engine: Engine
    session: Session


def mock_api_response() -> list[dict[str, Any]]:
    """
    Mock a Readwise 'Highlight EXPORT' endpoint ``response.json()["results"]`` output.

    Output contains one book with one highlight, each with one tag. Don't use a fixture
    so the content can be passed to other helper functions, to dynamically create
    expected values for parametrized tests. Use a function rather than a constant to
    ensure test isolation.

    Returns
    -------
    list[dict[str, Any]]
        A list containing one dictionary representing a Readwise book with one
        highlight.
    """
    return [
        {
            "user_book_id": 12345,
            "title": "book title",
            "is_deleted": False,
            "author": "name surname",
            "readable_title": "Book Title",
            "source": "web_clipper",
            "cover_image_url": "https://link/to/image",
            "unique_url": "http://the.source.url.ai",
            "summary": None,
            "book_tags": [{"id": 6969, "name": "arch_btw"}],
            "category": "books",
            "document_note": "A note added in Readwise Reader",
            "readwise_url": "https://readwise.io/bookreview/12345",
            "source_url": "http://the.source.url.ai",
            "asin": None,
            "highlights": [
                {
                    "id": 10,
                    "text": "The highlight text",
                    "location": 1000,
                    "location_type": "location",
                    "note": "document note",
                    "color": "yellow",
                    "highlighted_at": "2025-01-01T00:01:00",
                    "created_at": "2025-01-01T00:01:10",
                    "updated_at": "2025-01-01T00:01:20",
                    "external_id": None,
                    "end_location": None,
                    "url": None,
                    "book_id": 12345,
                    "tags": [{"id": 97654, "name": "favorite"}],
                    "is_favorite": False,
                    "is_discard": True,
                    "is_deleted": False,
                    "readwise_url": "https://readwise.io/open/10",
                }
            ],
        },
    ]


def mock_api_response_nested_validated(
    mock_api_response_with_a_single_book_fn: FetchFn = mock_api_response,
) -> list[dict[str, Any]]:
    """
    Mock the nested validation stage for mock_api_response containing a single book.

    Each object has validation fields added as if the object is valid.

    Returns
    -------
    list[dict[str, Any]]
        A list containing one dictionary representing a Readwise book with one
        highlight, each with one tag. This is a mock of the nested validation stage, so
        the objects are still in nested form, but with validation fields added.
    """
    validation = {"validated": True, "validation_errors": {}}
    mock_book = mock_api_response_with_a_single_book_fn()[0]
    mock_book.update(validation)
    mock_book["book_tags"][0].update(validation)
    mock_book["highlights"][0].update(validation)
    mock_book["highlights"][0]["tags"][0].update(validation)
    return [mock_book]


def flat_mock_api_response_nested_validated(
    nested_validated_mock_api_response_with_a_single_book_fn: ValidateNestedObjFn = mock_api_response_nested_validated,
) -> dict[str, list[dict[str, Any]]]:
    """
    Flatten a nested validated mock api response containing one book and one highlight.

    A nested validated api response is still in nested form but with validation fields
    added. This function flattens the objects and adds fk fields, matching the expected
    output from ``flatten_books_with_highlights()``. This is done manually to decouple
    the test logic.

    Returns
    -------
    dict[str, list[dict[str, Any]]]
        A dictionary where keys are the objects and values are list of those objects.
        Each list has only one object. E.g.
        ``{"books": [book], "highlights": [highlight] etc}``.
    """
    mock_book = nested_validated_mock_api_response_with_a_single_book_fn()[0]
    mock_book_tag = mock_book.pop("book_tags")[0]
    mock_highlight = mock_book.pop("highlights")[0]
    mock_highlight_tag = mock_highlight.pop("tags")[0]

    # Add foreign keys
    mock_book_tag["user_book_id"] = mock_book["user_book_id"]
    mock_highlight["book_id"] = mock_book["user_book_id"]
    mock_highlight_tag["highlight_id"] = mock_highlight["id"]

    return {
        "books": [mock_book],
        "book_tags": [mock_book_tag],
        "highlights": [mock_highlight],
        "highlight_tags": [mock_highlight_tag],
    }


def flat_mock_api_response_fully_validated(
    flattened_nested_validated_mock_api_response_single_book_fn: FlattenFn = flat_mock_api_response_nested_validated,
) -> dict[str, list[dict[str, Any]]]:
    """
    Create the final validated, flattened output expected by the database.

    Manually replicate the expected output from ``validate_flattened_objects``: this
    processes the API fields through the pydantic schema, updating the validation fields
    as appropriate, and doing any field processing.

    All objects are treated as valid.

    Returns
    -------
    dict[str, list[dict[str, Any]]]
        A dictionary where keys are the objects and values are list of those objects.
        Each list has only one object. E.g.
        ``{"books": [book], "highlights": [highlight] etc}``.
    """
    # Mock the pydantic field transformations.
    flattened_output = flattened_nested_validated_mock_api_response_single_book_fn()
    mock_highlight = flattened_output["highlights"][0]
    mock_highlight["highlighted_at"] = datetime(2025, 1, 1, 0, 1)
    mock_highlight["created_at"] = datetime(2025, 1, 1, 0, 1, 10)
    mock_highlight["updated_at"] = datetime(2025, 1, 1, 0, 1, 20)

    return flattened_output
