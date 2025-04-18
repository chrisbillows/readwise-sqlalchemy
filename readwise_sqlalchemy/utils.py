import json
import logging
import random
from pathlib import Path
from typing import Any

from sqlalchemy.inspection import inspect
from sqlalchemy.orm import DeclarativeBase

logger = logging.getLogger(__name__)


class FileHandler:
    """Handle file I/O."""

    @staticmethod
    def write_json(data: dict[Any, Any], file_path: Path) -> None:
        """Static method to write json."""
        with open(file_path, "w") as file_handle:
            json.dump(data, file_handle)
        logging.info(f"Written to: {file_path}")

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


def generate_random_number(num_of_digits: int) -> int:
    """
    Generate a random number n digits long.

    Parameters
    ----------


    """
    if num_of_digits < 0:
        raise ValueError("Number of digits must be greater than zero")
    lower = 10 ** (num_of_digits - 1)
    upper = 10**num_of_digits - 1
    return random.randint(lower, upper)
