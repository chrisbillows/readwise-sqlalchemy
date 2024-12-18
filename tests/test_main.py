import pytest


class TestUserConfig:
    @pytest.fixture
    def add_temp_user_config_to_self(self, synthetic_user_config) -> None:
        """Make `synthetic_user_config` available to all tests in the class."""
        self.user_config = synthetic_user_config

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
