"""
Report invalid database objects (books, highlights, book_tags) in a readable format.
"""

from typing import Optional, Protocol, cast, runtime_checkable

from readwise_sqlalchemy.config import UserConfig, fetch_user_config
from readwise_sqlalchemy.db_operations import get_session
from readwise_sqlalchemy.models import Book, BookTag, Highlight, HighlightTag


@runtime_checkable
class ValidatedModel(Protocol):
    """
    Protocol for ORM models that include validation fields.

    This protocol is used for static typing (e.g. with mypy) to indicate that an ORM
    model defines both `validated` and `validation_errors` attributes. One off usage
    in this module. If this reoccurs, consider creating a ValidatedBase orm class.
    """

    validated: bool
    validation_errors: dict[str, str]


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

    session.close()
