import pytest

from readwise_sqlalchemy.config import (
    MissingEnvironmentFile,
    UserConfig,
    fetch_user_config,
)


@pytest.mark.parametrize(
    "function_expected_is_true",
    [
        (lambda obj: obj.app_dir.is_dir()),
        (lambda obj: obj.env_file.exists()),
        (lambda obj: obj.readwise_api_token == "abc123"),
    ],
    ids=[
        "app_dir_is_directory",
        "env_file_exists",
        "readwise_api_token_is_set",
    ],
)
def test_init(mock_user_config, function_expected_is_true):
    assert function_expected_is_true(mock_user_config)


def test_user_config_objects_are_the_same_object_instance(mock_user_config):
    user_config_1 = fetch_user_config(mock_user_config.user_dir)
    user_config_2 = fetch_user_config(mock_user_config.user_dir)
    assert user_config_1 is user_config_2


def test_user_config_objects_are_different_instances(
    mock_user_config: UserConfig,
    mock_user_config_module_scoped: UserConfig,
):
    user_config_1 = fetch_user_config(mock_user_config.user_dir)
    user_config_2 = fetch_user_config(mock_user_config_module_scoped.user_dir)
    assert user_config_1 is not user_config_2


def test_missing_env_file_raises_exception(
    tmp_path: pytest.TempPathFactory,
):
    with pytest.raises(
        MissingEnvironmentFile,
        match="A '.env' file is expected in the '~/.config/rw-sql' directory.",
    ):
        UserConfig(tmp_path)
