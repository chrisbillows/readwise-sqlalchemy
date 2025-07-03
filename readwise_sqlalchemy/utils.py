import json
import logging
from pathlib import Path
from typing import Any, Optional, cast

from sqlalchemy.inspection import inspect
from sqlalchemy.orm import DeclarativeBase

from readwise_sqlalchemy.config import UserConfig, fetch_user_config
from readwise_sqlalchemy.db_operations import get_session
from readwise_sqlalchemy.integrations.readwise import fetch_from_export_api
from readwise_sqlalchemy.models import Book, BookTag, Highlight, HighlightTag
from readwise_sqlalchemy.types import ValidatedModel

logger = logging.getLogger(__name__)


class FileHandler:
    """Handle file I/O."""

    @staticmethod
    def write_json(
        data: dict[Any, Any] | list[dict[Any, Any]], file_path: Path
    ) -> None:
        """Static method to write json. Logs the file path to info."""
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


def fetch_real_user_data_json_for_end_to_end_testing(
    user_config: UserConfig = fetch_user_config(),
) -> None:
    """
    Fetch your real Readwise highlights and store locally as test data.

    FOR DEVELOPERS: This function fetches all your Readwise books and highlights and
    saves it as JSON for end-to-end testing.

    *THE FUNCTION MUST BE INVOKED BY YOU. YOUR DATA NEVER LEAVES YOUR MACHINE*

    Your data is saved locally as `~/readwise-sqlalchemy/my_readwise_highlights.json`.
    When pytest is invoked, if `my_readwise_highlights.json` exists, an end-to-end test
    is run. See the test at `tests/e2e/test_main.py`.

    To create `my_readwise_highlights.json` so the e2e tests runs, run:

    ```
    rwlp e2e-data
    ```

    Parameters
    ----------
    user_config: UserConfig, default = fetch_user_config()
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


def write_to_json_readwise_api_fetch_since_custom_date(
    books: list[dict[str, Any]], updates_since: str, user_config: UserConfig
) -> None:
    """
    Write the fetched Readwise data to a JSON file.

    Parameters
    ----------
    books : dict[str, Any]
        A dictionary containing the fetched data from Readwise.
    updates_since : str
        A string representing a datetime in ISO format (e.g., "2023-10-01T12:00:00").
    user_config : Optional[UserConfig]
        A UserConfig object.
    """
    file_dir = user_config.app_dir / "readwise_custom_fetches"
    file_dir.mkdir(parents=True, exist_ok=True)
    # Create a README to remind user the data is safe to delete.
    readme = file_dir / "README.md"
    if not readme.exists():
        readme.write_text(
            "User created custom fetches from the Readwise highlight export API "
            "endpoint.\nCan be deleted at anytime."
        )
    file_path = file_dir / f"custom_rw_fetch_{updates_since}.json"
    FileHandler.write_json(books, file_path)


def log_to_stdout_readwise_api_fetch_since_custom_date(
    books: list[dict[str, Any]], updates_since: str
) -> None:
    """
    Log the fetched Readwise data to stdout in a readable format.

    Parameters
    ----------
    books : dict[str, Any]
        A dictionary containing the fetched data from Readwise.
    updates_since : str
        A string representing a datetime in ISO format (e.g., "2023-10-01T12:00:00").
        The endpoint exports highlights with an "updated_at" field after this datetime.
    """
    logger.info(f"{len(books)} books updated since {updates_since}.")
    for book in books:
        highlights = book.get("highlights", [])
        logger.info(
            f"[bold red]book: {book['readable_title']}[/] (id:{book['user_book_id']})",
            extra={"markup": True},
        )
        logger.info(
            f"[bold red]h/lights: {len(highlights)} | cat: {book['category']} | author: "
            f"{book['author']} | source: {book['source']}[/]\n",
            extra={"markup": True},
        )
        for highlight in highlights:
            logger.info(
                f"text: {highlight['text'][:80].replace('\n', ' ').strip()} (id: "
                f"{highlight['id']})"
            )
            logger.info(
                f"h-lighted: {highlight.get('highlighted_at')}\n"
                f"created  : {highlight.get('created_at')}\n"
                f"updated  : {highlight.get('updated_at')}\n"
            )


def readwise_api_fetch_since_custom_date(
    updates_since: str,
    log: bool = False,
    user_config: Optional[UserConfig] = None,
) -> list[dict[str, Any]]:
    """
    Utility/debugging function to fetch new Readwise data since a custom datetime.

    Parameters
    ----------
    updates_since : str
        A string representing a datetime in ISO format (e.g., "2023-10-01T12:00:00").
        The endpoint exports highlights with an "updated_at" field after this datetime.
    log : bool, default = False
        If True, logs the fetched data to a file named
        ``custom_rw_fetch_<updates_since>.json``.
    user_config, default = None
        A UserConfig object.

    Raises
    ------
    ValueError
        If `updates_since` is not a valid ISO format string.

    Returns
    -------
    list[dict[str, Any]]
        A list containing the fetched data from Readwise since the specified datetime.

    """
    if not user_config:
        user_config = fetch_user_config()
    books = fetch_from_export_api(last_fetch=updates_since, user_config=user_config)
    log_to_stdout_readwise_api_fetch_since_custom_date(books, updates_since)
    if log:
        write_to_json_readwise_api_fetch_since_custom_date(
            books, updates_since, user_config
        )
    else:
        logger.info("Use --log-output to save the full API output to file.")
    return books


def list_invalid_db_objects(user_config: Optional[UserConfig] = None) -> None:
    """
    Report invalid database objects (books, highlights, book_tags) in a readable format.
    """
    if user_config is None:
        user_config = fetch_user_config()

    session = get_session(user_config.db_path)

    objs: list[type[ValidatedModel]] = cast(
        list[type[ValidatedModel]], [Book, BookTag, Highlight, HighlightTag]
    )
    invalids: list[tuple[str, ValidatedModel]] = []

    for obj in objs:
        results = session.query(obj).where(obj.validated.is_(False)).all()  # type: ignore[attr-defined]
        invalids.extend((obj.__name__, result) for result in results)

    print(f"{len(invalids)} invalid objects found:")
    for name, instance in invalids:
        print(f"[{name}] {instance}")
        for field, error in instance.validation_errors.items():
            print(f"  - {field}: {error}")

    session.close()
