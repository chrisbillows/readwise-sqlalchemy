from dataclasses import dataclass
from typing import Any

from sqlalchemy import Engine
from sqlalchemy.orm import Session


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

    Output contains one book with one highlight. Use a function rather than a constant
    to ensure test isolation.

    Returns
    -------
    list[dict[str, Any]]
        A list containing one dictionaries representing a Readwise book with highlights.
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
