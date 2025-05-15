from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from sqlalchemy.orm import Session

from readwise_sqlalchemy.config import UserConfig
from readwise_sqlalchemy.schemas import BookSchema

CheckDBFn = Callable[[Session, UserConfig], datetime | None]
FetchFn = Callable[[datetime | None], tuple[list[dict[str, Any]], datetime, datetime]]
FlattenFn = Callable[[list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]
LogSetupFn = Callable[[], None]
SessionFn = Callable[[str | Path], Session]
UpdateFn = Callable[[Session, list[BookSchema], datetime, datetime], None]
UpdateDbFlatObjFn = Callable[
    [Session, dict[str, list[dict[str, Any]]], datetime, datetime], None
]
ValidateFetchFn = Callable[
    [list[dict[str, Any]]], tuple[list[BookSchema], list[tuple[dict[str, Any], str]]]
]
ValidateNestedObjFn = Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
ValidateFlatObjFn = Callable[
    [dict[str, list[dict[str, Any]]]], dict[str, list[dict[str, Any]]]
]
