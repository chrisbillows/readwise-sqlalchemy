import logging
from datetime import datetime
from typing import Any

import requests
from pydantic import ValidationError
from sqlalchemy.orm import Session

from readwise_sqlalchemy.config import USER_CONFIG, UserConfig
from readwise_sqlalchemy.configure_logging import setup_logging
from readwise_sqlalchemy.db_operations import (
    DatabasePopulater,
    create_database,
    get_last_fetch,
    get_session,
)
from readwise_sqlalchemy.schemas import BookSchema
from readwise_sqlalchemy.types import (
    CheckDBFn,
    FetchFn,
    LogSetupFn,
    SessionFn,
    UpdateFn,
    ValidateFetchFn,
)

logger = logging.getLogger(__name__)


def fetch_from_export_api(
    last_fetch: None | str = None,
    user_config: UserConfig = USER_CONFIG,
) -> list[dict[str, Any]]:
    """
    Fetch highlights from the Readwise Highlight EXPORT endpoint.

    Code is per the documentation. See: https://readwise.io/api_deets

    Parameters
    ----------
    last_fetch: str, default = None
        An ISO formatted datetime string E.g. '2024-11-09T10:15:38.428687' indicating
        the time highlights have previously been fetched up to.
    user_config: UserConfig, default = USER_CONFIG
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


def check_database(
    session: Session, user_config: UserConfig = USER_CONFIG
) -> None | datetime:
    """
    If the db exists, return the last fetch time, otherwise create the db.

    Parameters
    ----------
    session: Session
        A SQL alchemy session connected to a database.
    user_config: UserConfig, default = USER_CONFIG
        A User Config object.

    Returns
    -------
    None | datetime
        None if the database doesn't exist. If the database exists, the time the last
        fetch was completed as a datetime object.
    """
    if user_config.db_path.exists():
        logger.info("Database exists")
        last_fetch = get_last_fetch(session)
        logger.info(f"Last fetch: {last_fetch}")
        return last_fetch
    else:
        logger.info(f"Creating database at {user_config.db_path}")
        create_database(user_config.db_path)
        return None


def datetime_to_isoformat_str(datetime: datetime) -> str:
    """
    Convert a datetime object to an ISO 8601 string.

    This functions wraps the Pathlib method call for testability and to easily assess
    compatibility with Readwise Highlight Export API.

    Parameters
    ----------
    datetime: datetime
        A valid datetime object.

    Returns
    -------
    str
        An ISO 8601 formatted datetime string E.g. '2024-11-09T10:15:38.428687'.
    """
    return datetime.isoformat()


def fetch_books_with_highlights(
    last_fetch: None | datetime,
) -> tuple[list[dict[str, Any]], datetime, datetime]:
    """
    Runner for fetching Readwise Highlights from the Readwise API.

    Parameters
    ----------
    last_fetch: None | datetime
        A datetime object indicating the time highlights have previously been fetched
        up to.

    Returns
    -------
    tuple[list[dict[str, Any]], datetime, datetime]
        A tuple consisting of:
            - data: a list of dictionaries where each item is a book with highlights
            - start_new_fetch: start of the most recent fetch as a datetime
            - end_new_fetch: end of the most recent fetch as a datetime
    """
    last_fetch_str: str | None = None

    if last_fetch:
        last_fetch_str = datetime_to_isoformat_str(last_fetch)

    start_new_fetch = datetime.now()
    data = fetch_from_export_api(last_fetch_str)
    end_new_fetch = datetime.now()
    logger.info(f"Fetch contains highlights for {len(data)} books/articles/tweets etc.")
    return (data, start_new_fetch, end_new_fetch)


def validation_ensure_list(
    obj: dict[str, Any], field: str, parent_label: str
) -> list[str]:
    """
    Ensure a field is a list. Fix if needed and return any validation errors.

    Parameters
    ----------
    obj: dict
        A dictionary-like object.
    field: str
        The field to check.
    parent_label: str
        A label for the parent object (for error messages). E.g. "book" or "highlight".

    Returns
    -------
    list[str]
        A list of validation errors. Empty if the field is valid.
    """
    errors = []
    if obj.get(field) is None:
        obj[field] = []
        errors.append(f"No {field} found in {parent_label}")
    elif not isinstance(obj[field], list):
        errors.append(
            f"{field} not stored, not a list in {parent_label}. Value: {obj[field]}"
        )
        obj[field] = []
    return errors


def validation_annotate_validated(obj: dict[str, Any], errors: list[str]) -> None:
    """
    Set `validated` and `validation_errors` fields on any dict-like object.

    Parameters
    ----------
    obj: dict
        A dictionary-like object.
    errors: list[str]
        A list of validation errors. Empty if the object is valid.
    """
    obj["validated"] = not errors
    obj["validation_errors"] = errors


def validation_highlight_book_id(
    highlight: dict[str, Any], book_user_book_id: int
) -> list[str]:
    """
    Ensure highlight.book_id matches its parent book.user_book_id.

    Fix if needed and return any validation errors.

    #TODO: Finish docstring


    """
    errors = []
    if highlight.get("book_id") != book_user_book_id:
        errors.append(
            f"Highlight book_id {highlight.get('book_id')} does not match "
            f"book user_book_id {book_user_book_id}"
        )
        highlight["book_id"] = book_user_book_id
    return errors


def validation_nested_obj_layer(
    raw_books: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    First validation layer: validate nested Readwise objects.

    Check and fix essential aspects of nested objects to avoid downstream errors once
    the API data is flattened. All books are assumed to have a "user_book_id" field.

    Each object is tagged with:
    - `validated`: True or False
    - `validation_errors`: a list of errors (empty if valid)

    Parameters
    ----------
    raw_books : list[dict]
        A list of raw dicts from the Readwise API.

    Returns
    -------
    raw_books : list[dict]
        A list of raw dicts from the Readwise API. Each object has a `validated` field
        set to True or False, and a `validation_errors` field containing a list of
        errors (empty if the object is valid). False will indicate the object failed
        validation in any validation layer.

    Notes
    -----
    - The validation checks:
        - Each book has a list of highlights and book_tags. If the field is missing,
          set to an empty list. If the field value is a type other than a list, put
          the field value into a list.
        - Each highlight's `book_id` matches the book's `user_book_id`.
        - Each highlight has a list of tags. If the field is missing, set to an empty
          list. If the field value is a type other than a list, put the field value into
          a list.
    - The validation checks are not strict. They are designed to ensure that the data
      is in a format that can be processed downstream. The checks are not exhaustive
      and do not cover all possible edge cases. The goal is to include as much data as
      possible while still being able to promise type safety when using data from the
      database.

    """
    for book in raw_books:
        book_errors = []

        # Highlights and book_tags must exist and be lists
        book_errors += validation_ensure_list(book, "highlights", "book")
        book_errors += validation_ensure_list(book, "book_tags", "book")

        # Validate each book_tag (no strict checks yet)
        for tag in book["book_tags"]:
            validation_annotate_validated(tag, [])

        # Validate each highlight and its tags
        for highlight in book["highlights"]:
            highlight_errors = []

            highlight_errors += validation_highlight_book_id(
                highlight, book["user_book_id"]
            )
            highlight_errors += validation_ensure_list(highlight, "tags", "highlight")

            for tag in highlight["tags"]:
                validation_annotate_validated(tag, [])

            validation_annotate_validated(highlight, highlight_errors)

        validation_annotate_validated(book, book_errors)

    return raw_books


def validate_books_with_highlights(
    raw_books: list[dict[str, Any]],
) -> tuple[list[BookSchema], list[tuple[dict[str, Any], str]]]:
    """
    Attempt to convert raw book dicts to Pydantic BookSchema models.

    Parameters
    ----------
    raw_books : list[dict]
        A list of raw dicts from the Readwise API.

    Returns
    -------
    tuple
        - A list of successfully validated BookSchema instances.
        - A list of tuples containing (invalid dict, error message).
    """
    valid_books = []
    failed_books = []

    for raw_book in raw_books:
        try:
            book_as_schema = BookSchema(**raw_book)
            valid_books.append(book_as_schema)
        except ValidationError as err:
            error_msg = str(err)
            failed_books.append((raw_book, error_msg))
            logging.warning(
                "Validation failed for book with title '"
                f"{raw_book.get('title', '[no title]')}'. Error: {error_msg}"
            )

    return valid_books, failed_books


def update_database(
    session: Session,
    validated_books: list[BookSchema],
    start_fetch: datetime,
    end_fetch: datetime,
) -> None:
    """
    Update the database.

    Parameters
    ----------
    session: Session
        A SQL alchemy session connected to a database.
    validated_books: list[BookSchema]
        A list of books with highlights fetched from the Readwise Highlight EXPORT
        endpoint.
    start_fetch: datetime
        The time the fetch was called.
    end_fetch: datetime
        The time the fetch was completed.
    """
    logger.info("Updating database")
    dbp = DatabasePopulater(session, validated_books, start_fetch, end_fetch)
    dbp.populate_database()
    logger.info("Database contains all Readwise highlights to date")


def run_pipeline(
    user_config: UserConfig = USER_CONFIG,
    setup_logging_func: LogSetupFn = setup_logging,
    get_session_func: SessionFn = get_session,
    check_db_func: CheckDBFn = check_database,
    fetch_func: FetchFn = fetch_books_with_highlights,
    validate_func: ValidateFetchFn = validate_books_with_highlights,
    update_db_func: UpdateFn = update_database,
) -> None:
    """
    Orchestrate the end-to-end Readwise data sync process.

    Creates a new database and fetches all highlights, or gets the last fetch datetime
    and fetches only new highlights.

    Use dependency injection for functions for simplified testing.

    Parameters
    ----------
    user_config : UserConfig, optional, default = USER_CONFIG
        Configuration object.
    setup_logging_func: LogSetupFn, optional, default = setup_logging()
        A function that sets up application logging.
    get_session_func: SessionFn, optional, get_session()
        A function that returns a SQLAlchemy database Session.
    check_db_func: CheckDBFn, optional, default = check_database()
        A function that creates the database or returns the last fetch datetime (or
        None if it just creates the db).
    fetch_func: FetchFn, optional, default = fetch_books_with_highlights()
        Function that fetches highlights and returns them as a tuple with the start
        and end times of the fetch as datetimes.
    validate_func: ValidateFetchFn, default = validate_books_with_highlights()
        A function that validates an API response, returning lists of valid and failed
        items.
    update_func: UpdateFn, optional, default = update_database()
        Function that populates the database with fetched highlights.
    """
    setup_logging_func()
    session = get_session_func(user_config.db_path)
    last_fetch = check_db_func(session, user_config)
    data, start_fetch, end_fetch = fetch_func(last_fetch)
    valid_books, failed_books = validate_func(data)
    update_db_func(session, valid_books, start_fetch, end_fetch)


def main(user_config: UserConfig = USER_CONFIG) -> None:
    """
    Main function that runs with the entry point.

    Parameters
    ----------
    user_config
        A UserConfig object.
    """
    run_pipeline(user_config)


if __name__ == "__main__":
    main()
