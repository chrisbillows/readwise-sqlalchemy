import json
import logging
from pathlib import Path
from typing import Any

from sqlalchemy.inspection import inspect
from sqlalchemy.orm import DeclarativeBase

from readwise_sqlalchemy.config import USER_CONFIG, UserConfig
from readwise_sqlalchemy.main import fetch_from_export_api

logger = logging.getLogger(__name__)


class FileHandler:
    """Handle file I/O."""

    @staticmethod
    def write_json(
        data: dict[Any, Any] | list[dict[Any, Any]], file_path: Path
    ) -> None:
        """Static method to write json."""
        with open(file_path, "w") as file_handle:
            json.dump(data, file_handle)
        logger.info(f"Written to: '{file_path}'")

    @staticmethod
    def read_json(file_path: Path | str) -> Any:
        """Static method to read json."""
        with open(file_path, "r") as file_handle:
            content = json.load(file_handle)
        return content


def get_columns_and_values(orm_mapped_obj: DeclarativeBase) -> dict[str, Any]:
    """
    Extracts only the mapped database columns from a SQLAlchemy ORM instance.

    Parameters
    ----------
    orm_mapped_obj: DeclarativeBase
        A mapped object.
    """
    return {
        column.key: getattr(orm_mapped_obj, column.key)
        for column in inspect(orm_mapped_obj).mapper.column_attrs
    }


def create_real_user_data_json_for_end_to_end_testing(
    user_config: UserConfig = USER_CONFIG,
) -> None:
    """
    Fetch your real Readwise highlights and store locally as test data.

    FOR DEVELOPERS: This function fetches all your Readwise books and highlights and
    saves it as JSON for end-to-end testing.

    *THE FUNCTION MUST BE INVOKED BY YOU. YOUR DATA NEVER LEAVES YOUR MACHINE*

    Your data is saved locally as `~/readwise-sqlalchemy/my_readwise_highlights.json`.
    When pytest is invoked, if `my_readwise_highlights.json` exists, an end-to-end test
    is run. See the test at `tests/e2e/test_main.py`.

    To create `my_readwise_highlights.json` so the e2e tests runs, navigate to
    the project directory in your CLI and run:

    ```
    python3 -m readwise_sqlalchemy.utils
    ```

    This runs the `if __name__ == "__main__"` block at the bottom of this file.

    Parameters
    ----------
    user_config: UserConfig, default = USER_CONFIG
        A UserConfig object.
    """
    target_file_path = user_config.app_dir / "my_readwise_highlights.json"

    logger.info(
        "Hello Developer! Fetching your Readwise highlights and writing to JSON."
    )
    api_content = fetch_from_export_api(last_fetch=None, user_config=user_config)
    FileHandler.write_json(api_content, target_file_path)

    logger.info(
        "When you next run pytest, the e2e test should run automatically. P.S.YOUR "
        "DATA NEVER LEAVES YOUR MACHINE."
    )


if __name__ == "__main__":
    create_real_user_data_json_for_end_to_end_testing()
