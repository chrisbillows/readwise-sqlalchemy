import pytest

from readwise_sqlalchemy.main import UserConfig


@pytest.fixture
def temp_user_config(tmp_path: pytest.TempPathFactory) -> None:
    """Create a temporary user configuration."""
    temp_application_dir = tmp_path / "readwise-sqlalchemy-application"
    temp_env_file = temp_application_dir / ".env"

    temp_application_dir.mkdir()
    temp_env_file.touch()
    temp_env_file.write_text("READWISE_API_TOKEN = 'abc123'")

    return UserConfig(temp_application_dir)
