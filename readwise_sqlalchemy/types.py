from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from sqlalchemy.orm import Session

from readwise_sqlalchemy.config import UserConfig

LogSetupFn = Callable[[], None]
SessionFn = Callable[[str | Path], Session]
CheckDBFn = Callable[[Session, UserConfig], datetime | None]
FetchFn = Callable[[datetime | None], tuple[list[dict[str, Any]], datetime, datetime]]
UpdateFn = Callable[[Session, list[dict[str, Any]], Any, Any], None]
