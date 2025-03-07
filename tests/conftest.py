"""
Pytest fixtures and reusable test data.

Fixtures
--------
mock_book: dict
    Mock a valid book dictionary returned by the Readwise 'Highlight EXPORT' endpoint.

mock_highlight: dict
    Mock a valid highlight dictionary returned by the Readwise 'Highlight EXPORT'
    endpoint as part of a book dictionary.
    
mock_highlight_tag: dict


mock_user_config: UserConfig
    Mock a valid readwise_sqlalchemy user configuration with a 'tmp_path' as user dir.

Constants
---------
Construct valid and invalid dictionaries replicating the Readwise 'Highlight EXPORT'
endpoint.

BOOK_SCHEMA_VARIANTS:
    Construct valid and invalid mock book dictionaries.

HIGHLIGHT_SCHEMA_VARIANTS:
    Construct valid and invalid mock highlights dictionaries.
"""

import pytest

from readwise_sqlalchemy.main import UserConfig


@pytest.fixture
def mock_book() -> dict:
    """Valid Readwise mock book."""
    fields = BOOK_SCHEMA_VARIANTS.keys()
    return {field: BOOK_SCHEMA_VARIANTS[field]["value_valid"] for field in fields}


@pytest.fixture
def mock_highlight() -> dict:
    """Valid Readwise mock highlight."""
    fields = HIGHLIGHT_SCHEMA_VARIANTS.keys()
    return {field: HIGHLIGHT_SCHEMA_VARIANTS[field]["value_valid"] for field in fields}


@pytest.fixture
def mock_highlight_tags() -> dict:
    """Valid Readwise mock highlight tags."""
    fields = HIGHLIGHT_TAGS_SCHEMA_VARIANTS.keys()
    return {field: HIGHLIGHT_TAGS_SCHEMA_VARIANTS[field]["value_valid"] for field in fields}


@pytest.fixture
def mock_user_config(tmp_path: pytest.TempPathFactory) -> UserConfig:
    """Return a temporary readwise-sqlalchemy user configuration.

    Use a `tmp_path` as the User's home directory. Create the directory and create the
    required .env file with required synthetic data.
    """
    temp_application_dir = tmp_path / "readwise-sqlalchemy-application"
    temp_application_dir.mkdir()

    temp_env_file = temp_application_dir / ".env"
    temp_env_file.touch()
    temp_env_file.write_text("READWISE_API_TOKEN = 'abc123'")
    return UserConfig(temp_application_dir)


# 'value_invalid_type' is an example of one possible invalid type. It is not exhaustive.
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
        "value_valid": ["yellow", "green"],
        "value_invalid_type": [1, 2, 3],
        "nullable": True,  # Field validator converts to []
    },
    "category": {
        "value_valid": "books",
        "value_invalid_type": "youtube",
        "nullable": False,
    },
        "document_note": {
        "value_valid": "A string",
        "value_invalid_type": 1000,
        "nullable": True,
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


# 'value_invalid_type' is an example of one possible invalid type. It is not exhaustive.
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
        "value_valid": "2025-01-01T01:02:03.456Z",
        "value_invalid_type": "2024-13-01T10:20:30.123Z",
        "nullable": True,
    },
    "created_at": {
        "value_valid": "2025-01-01T01:02:03.456Z",
        "value_invalid_type": "2024-13-01T10:20:30.123Z",
        "nullable": True,
    },
    "updated_at": {
        "value_valid": "2025-01-01T01:02:03.456Z",
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
    "tags": {
        "value_valid": [],
        "value_invalid_type": [{'unexpected_field': 'value'}],
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

# 'value_invalid_type' is an example of one possible invalid type. It is not exhaustive.
HIGHLIGHT_TAGS_SCHEMA_VARIANTS = {
    "id": {
        "value_valid": 123456,
        "value_invalid_type": "one",
        "nullable": True,
    },
    "name": {
        "value_valid": "label",
        "value_invalid_type": 1,
        "nullable": True,
    }
}
