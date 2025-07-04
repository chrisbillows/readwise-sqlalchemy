from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Protocol, Type, TypeVar, runtime_checkable

from sqlalchemy.orm import Session

from readwise_local_plus.config import UserConfig
from readwise_local_plus.models import Base, ReadwiseBatch

CheckDBFn = Callable[[Session, UserConfig], datetime | None]
FetchFn = Callable[[datetime | None], tuple[list[dict[str, Any]], datetime, datetime]]
FlattenFn = Callable[[list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]
SessionFn = Callable[[str | Path], Session]
UpdateDbFlatObjFn = Callable[
    [Session, dict[str, list[dict[str, Any]]], datetime, datetime], None
]
ValidateNestedObjFn = Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
ValidateFlatObjFn = Callable[
    [dict[str, list[dict[str, Any]]]], dict[str, list[dict[str, Any]]]
]


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


@runtime_checkable
class VersionableORM(Protocol):
    def dump_column_data(self, exclude: set[str] = ...) -> dict[str, object]: ...

    batch_id: int
    batch: ReadwiseBatch
    version_class: Type[Base]


VersionableT = TypeVar("VersionableT", bound=VersionableORM)
