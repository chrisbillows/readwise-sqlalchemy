import logging
from typing import Any

import requests

from readwise_local_plus.config import UserConfig, fetch_user_config

logger = logging.getLogger(__name__)


def fetch_from_export_api(
    last_fetch: None | str = None,
    user_config: UserConfig = fetch_user_config(),
) -> list[dict[str, Any]]:
    """
    Fetch highlights from the Readwise Highlight EXPORT endpoint.

    Code is per the documentation. See: https://readwise.io/api_deets

    Parameters
    ----------
    last_fetch: str, default = None
        An ISO formatted datetime string E.g. '2024-11-09T10:15:38Z'. The seems
        to filter on a highlights "updated_at" value. The above example would therefore
        exclude a highlight with an "updated_at" value of '2024-11-09T10:15:37.999Z'.
    user_config: UserConfig, default = fetch_user_config()
        A User Configuration object.

    Returns
    -------
    list[dict[str, Any]]
        A list of dicts where each dict represents a "book". (Highlights are always
        exported within a book).

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
        if last_fetch:
            params["updatedAfter"] = last_fetch
        logger.info("Making export api request with params " + str(params) + "...")
        response = requests.get(
            url="https://readwise.io/api/v2/export/",
            params=params,
            # Readwise Docs specify `verify=False`. `True` used to suppress warnings.
            headers={"Authorization": f"Token {user_config.readwise_api_token}"},
            verify=True,
        )
        full_data.extend(response.json()["results"])
        next_page_cursor = response.json().get("nextPageCursor")
        if not next_page_cursor:
            break
    return full_data
