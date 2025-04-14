import logging
from datetime import datetime
from typing import Any

import requests

from readwise_sqlalchemy.config import USER_CONFIG, UserConfig
from readwise_sqlalchemy.configure_logging import setup_logging
from readwise_sqlalchemy.db_operations import (
    DatabasePopulater,
    create_database,
    get_session,
    query_get_last_fetch,
)

logger = logging.getLogger(__name__)


def fetch_from_export_api(
    user_config: UserConfig = USER_CONFIG, updated_after: None | str = None
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
        logger.info("Making export api request with params " + str(params) + "...")
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


def main(user_config: UserConfig = USER_CONFIG) -> None:
    """Main function ran with `rw` entry point.

    Create a database and populate it with all readwise data or, if the database already
    exists, fetch all data since the last fetch.
    """
    setup_logging()

    session = get_session(USER_CONFIG.DB)
    last_fetch = None

    if USER_CONFIG.DB.exists():
        logger.info("Database exists")
        last_fetch = query_get_last_fetch(session)
        logger.info(f"Last fetch: {last_fetch}")
    else:
        logger.info("Creating database")
        create_database(USER_CONFIG.DB)

    logger.info("Updating database")
    start_fetch = datetime.now()
    data = fetch_from_export_api()
    end_fetch = datetime.now()
    dbp = DatabasePopulater(session, data, start_fetch, end_fetch)
    logger.info(f"Fetch contains highlights for {len(data)} books/articles/tweets etc.")
    dbp.populate_database()
    logger.info("Database contains all Readwise highlights to date")


if __name__ == "__main__":
    main()
