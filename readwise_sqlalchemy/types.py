from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Protocol, Type, TypeVar, runtime_checkable

from sqlalchemy.orm import Session

from readwise_sqlalchemy.config import UserConfig
from readwise_sqlalchemy.models import Base, ReadwiseBatch

CheckDBFn = Callable[[Session, UserConfig], datetime | None]
FetchFn = Callable[[datetime | None], tuple[list[dict[str, Any]], datetime, datetime]]
FlattenFn = Callable[[list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]
LogSetupFn = Callable[[], None]
SessionFn = Callable[[str | Path], Session]
UpdateDbFlatObjFn = Callable[
    [Session, dict[str, list[dict[str, Any]]], datetime, datetime], None
]
ValidateNestedObjFn = Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
ValidateFlatObjFn = Callable[
    [dict[str, list[dict[str, Any]]]], dict[str, list[dict[str, Any]]]
]


@runtime_checkable
class VersionableORM(Protocol):
    def dump_column_data(self, exclude: set[str] = ...) -> dict[str, object]: ...

    batch_id: int
    batch: ReadwiseBatch
    version_class: Type[Base]


VersionableT = TypeVar("VersionableT", bound=VersionableORM)
