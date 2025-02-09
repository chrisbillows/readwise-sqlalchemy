import pytest

from readwise_sqlalchemy.main import UserConfig


@pytest.fixture
def synthetic_user_config(tmp_path: pytest.TempPathFactory) -> None:
    """Create a temporary user configuration.

    Use a `tmp_path` as the User's home directory. Create the directory and create the
    required .env file with required synthetic data.
    """
    temp_application_dir = tmp_path / "readwise-sqlalchemy-application"
    temp_application_dir.mkdir()

    temp_env_file = temp_application_dir / ".env"
    temp_env_file.touch()
    temp_env_file.write_text("READWISE_API_TOKEN = 'abc123'")

    print(1.1, temp_application_dir)
    print(1.2, temp_env_file)
    print(1.3, temp_env_file.read_text())

    return UserConfig(temp_application_dir)
