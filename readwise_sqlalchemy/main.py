import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from readwise_sqlalchemy.db_operations import (
    create_database,
    get_session,
    query_get_last_fetch,
)


class MissingEnvironmentFile(Exception):
    """Custom exception if environment file not available."""

    pass


class UserConfig:
    """Object containing user configuration information."""

    def __init__(
        self, application_dir: Path = Path.home() / "readwise-sqlalchemy-application"
    ):
        """
        Initialise object.

        Attributes
        ----------
        APPLICATION_DIR: pathlib.Path

        ENV_FILE: pathlib.Path

        READWISE_API_TOKEN: str

        """
        self.APPLICATION_DIR: Path = application_dir
        self.APPLICATION_DIR.mkdir(exist_ok=True)
        self.ENV_FILE: Path = self.APPLICATION_DIR / ".env"
        self.load_environment_variables_file()
        self.READWISE_API_TOKEN: str | None = os.getenv("READWISE_API_TOKEN")
        self.DB: Path = self.APPLICATION_DIR / "readwise.db"

    def load_environment_variables_file(self) -> None:
        """
        Load the `.env` file.

        Raises
        ------
        MissingEnvironmentFile
            If the .env file is not in the expected location.
        """
        if self.ENV_FILE.exists():
            load_dotenv(self.ENV_FILE)
        else:
            raise MissingEnvironmentFile(
                "A `.env` file is expected in the `~/readwise-sqlalchemy-application` "
                "directory."
            )


class FileHandler:
    """Handle file I/O."""

    @staticmethod
    def write_json(data: dict[Any, Any], file_path: Path) -> None:
        """Static method to write json."""
        with open(file_path, "w") as file_handle:
            json.dump(data, file_handle)
        print(f"Written to: {file_path}")

    @staticmethod
    def read_json(file_path: Path | str) -> Any:
        """Static method to read json."""
        with open(file_path, "r") as file_handle:
            content = json.load(file_handle)
        return content


def fetch_from_export_api(
    user_config: UserConfig, updated_after: None | str = None
) -> list[dict[Any, Any]]:
    """Fetch highlights from the Readwise 'Highlight EXPORT' endpoint.

    Code is per the documentation. See: https://readwise.io/api_deets

    Parameters
    ----------
    updated_after: str = None
        An ISO formatted datetime E.g. '2024-11-09T10:15:38.428687'

    Returns
    -------
    list[dict]
    A list of dicts where each dict represents a "book".

    Notes
    -----
    Readwise uses 'book' for all types of highlight source. They are split into these
    categories: `{'tweets', 'books', 'articles', 'podcasts'}`

    Each 'book' has the following keys:

    ```
    book_keys = [
        'user_book_id', 'title', 'author', 'readable_title', 'source', 'cover_image_url',
        'unique_url', 'summary', 'book_tags', 'category', 'document_note',
        'readwise_url', 'source_url', 'asin', 'highlights']
    ```

    `'highlights'` is a list of dicts where each dict is a highlight. Each highlight
    contains the following keys:

    ```
    highlight_keys = [
        'id', 'text', 'location', 'location_type', 'note', 'color', 'highlighted_at',
        'created_at', 'updated_at', 'external_id', 'end_location', 'url', 'book_id',
        'tags', 'is_favorite', 'is_discard', 'readwise_url'
        ]
    ```
    """
    full_data = []
    next_page_cursor = None
    while True:
        params = {}
        if next_page_cursor:
            params["pageCursor"] = next_page_cursor
        if updated_after:
            params["updatedAfter"] = updated_after
        print("Making export api request with params " + str(params) + "...")
        response = requests.get(
            url="https://readwise.io/api/v2/export/",
            params=params,
            # Readwise Docs specify `verify=False`. `True` used to suppress warnings.
            headers={"Authorization": f"Token {user_config.READWISE_API_TOKEN}"},
            verify=True,
        )
        full_data.extend(response.json()["results"])
        next_page_cursor = response.json().get("nextPageCursor")
        if not next_page_cursor:
            break
    return full_data


def main() -> None:
    """Main function ran with `rw` entry point.

    Create a database and populate it with all readwise data or, if the database already
    exists, fetch all data since the last fetch.
    """
    user_config = UserConfig()
    session = get_session(user_config.DB)
    last_fetch = None

    if user_config.DB.exists():
        print("Database exists")
        last_fetch = query_get_last_fetch(session)
        print("Last fetch:", last_fetch)
    else:
        print("Creating database")
        create_database(user_config.DB)

    print("Updating database")
    start_fetch = datetime.now()
    # data = fetch_from_export_api()
    # TODO: Replace with real API call
    data = FileHandler.read_json(
        "/Users/chrisbillows/Documents/CODE/MY_GITHUB_REPOS/readwise-sqlalchemy/tests/data/real/sample_updated_25th_nov_to_26th_nov.json"
    )
    end_fetch = datetime.now()
    # dbp = DatabasePopulater(session, data, start_fetch, end_fetch)
    # print(f"Fetch contains highlights for {len(data)} books/articles/tweets etc.")
    # dbp.populate_database()
    # print("Database contains all Readwise highlights to date")


if __name__ == "__main__":
    main()
