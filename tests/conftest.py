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
