from os import mkdir
import pytest

from readwise_sqlalchemy.main import UserConfig


class TestUserConfig:
    @pytest.fixture
    def temp_user_config(self, tmp_path: pytest.TempPathFactory) -> None:
        """Create a temporary user configuration."""
        temp_application_dir = tmp_path / "readwise-sqlalchemy-application"
        temp_env_file = temp_application_dir / ".env"

        temp_application_dir.mkdir()
        temp_env_file.touch()
        temp_env_file.write_text("READWISE_API_TOKEN = 'abc123'")

        self.user_config = UserConfig(temp_application_dir)

    @pytest.mark.parametrize(
        "expected_is_true",
        [
            ("APPLICATION_DIR", lambda obj: obj.APPLICATION_DIR.is_dir()),
            ("ENV_FILE", lambda obj: obj.ENV_FILE.exists()),
            ("READWISE_API_TOKEN", lambda obj: obj.READWISE_API_TOKEN == "abc123"),
        ],
    )
    def test_init(self, expected_is_true):
        assert expected_is_true
