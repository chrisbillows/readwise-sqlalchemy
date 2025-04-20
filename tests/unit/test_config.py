import pytest


class TestUserConfig:
    @pytest.fixture
    def add_temp_user_config_to_self(self, mock_user_config) -> None:
        """Make `synthetic_user_config` available to all tests in the class."""
        self.user_config = mock_user_config

    @pytest.mark.parametrize(
        "expected_is_true",
        [
            ("app_dir", lambda obj: obj.APPLICATION_DIR.is_dir()),
            ("env_file", lambda obj: obj.ENV_FILE.exists()),
            ("readwise_api_token", lambda obj: obj.READWISE_API_TOKEN == "abc123"),
        ],
    )
    def test_init(self, expected_is_true):
        assert expected_is_true
