import json
from pathlib import Path
import random
from typing import Any, Union

import faker
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import DeclarativeBase


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


def generate_api_response_data_readwise_highlight_export_endpoint(
    num_of_books: int, max_highlights: int = 20
) -> list[dict[str, Any]]:
    """
    Dynamically generate a mock Readwise 'Highlight EXPORT' endpoint response.

    Generates a book with mock values and a random number of mock highlights. Generates
    the API response as if already loaded into Python with the JSON module.

    Create test data as required.

    Parameters
    ----------
    num_of_books : int
        The number of mock books to include in the mock API response.
    max_highlights : int
        The max number of mock highlights to include per book. Defaults to 20. Minimum
        is one highlight per book.
    """
    fake = faker.Faker()
    mock_api_response = []
    for _ in range(num_of_books):
        # Declare type for mypy
        book: dict[str, Any]
        book = {
            "user_book_id": generate_random_number(8),
            "title": fake.sentence(nb_words=6),
            "author": fake.name(),
            "source": random.choice(
                [
                    "reader",
                    "snipd",
                    "twitter",
                    "ibooks",
                    "kindle",
                    "web_clipper",
                    "pdf",
                    "api_article",
                    "pocket",
                    "airr",
                    "podcast",
                    "hypothesis",
                ]
            ),
            "cover_image_url": fake.image_url(),
            "unique_url": fake.url() if random.random() < 0.2 else None,
            "summary": fake.text(max_nb_chars=200) if random.random() < 0.2 else None,
            "book_tags": fake.words(
                random.choices([0, 1, 2, 3], weights=[3, 1, 1, 1])[0]
            ),
            "category": random.choice(["books", "articles", "tweets", "podcasts"]),
            "document_note": fake.sentence() if random.choice([True, False]) else "",
            "asin": fake.isbn10(),
            "highlights": [],
        }
        book["readable_title"] = book["title"].title()
        book["source_url"] = "" if book["category"] == "books" else fake.url()
        book["readwise_url"] = f"https://readwise.io/bookreview/{book['user_book_id']}"

        for _ in range(random.randint(1, max_highlights)):
            highlight = {
                "id": generate_random_number(10),
                "text": fake.paragraph(30),
                "location": random.randint(-10000, 10000),
                "location_type": random.choice(
                    ["offset", "time_offset", "order", "location", "page"]
                ),
                "note": fake.paragraph(30) if random.random() < 0.2 else "",
                "color": random.choice(
                    ["yellow", "pink", "orange", "blue", "purple", "green"]
                ),
                "highlighted_at": fake.iso8601(),
                "created_at": fake.iso8601(),
                "updated_at": fake.iso8601(),
                "external_id": random.choice(
                    [
                        fake.safe_hex_color(),  # Random selection of ref numbers.
                        fake.uuid4(),
                        fake.swift8(),
                        fake.iban(),
                    ]
                )
                if random.random() < 0.2
                else None,
                "end_location": None,
                "url": fake.url(),
                "book_id": book["user_book_id"],
                "tags": [
                    {"id": generate_random_number(6), "name": "yellow"},
                    {"id": 456, "name": "green"},
                ],
                "is_favorite": random.choice([True, False]),
                "is_discard": random.choice([True, False]),
            }
            highlight["readwise_url"] = f"https://readwise.io/open/{highlight['id']}"
            book["highlights"].append(highlight)
        mock_api_response.append(book)
    return mock_api_response
