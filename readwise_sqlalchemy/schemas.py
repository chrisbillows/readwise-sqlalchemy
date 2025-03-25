"""
Pydantic schema for verifying data types.

The schema defines the datatypes to expect for each field - while being permissive
enough to accept ALL real Readwise user data. The objective is to make reliable promises
about datatypes - not to reject "bad" data.

For example, URL fields are verified as strings, not URLs. This indicates a valid URL
cannot be guaranteed. Downstream use of the data may benefit from additional validation
or error handling. (User data contained one record that failed URL validation).

Note
----
- Where a schema attribute has a defined ``Field`` - e.g. ``Field(max_length=8191)``
  this indicates there is documentation for the field. (See:
  https://readwise.io/api_deets). Undocumented assumptions are noted as inline comments.
- All models pass two keyword arguments:
    - ``strict=True``: Enables strict enforcement of types for all fields. Individual
      fields are opted out as required.
    - ``extra=forbid``: Enables unexpected, undefined fields to error. Pydantic schema
      accept unexpected fields by default.
"""

import json
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class HighlightTagsSchema(BaseModel, extra="forbid", strict=True):
    """
    Validate 'tags' fields in a HighlightSchema highlight.
    """

    # Undocumented. Seems likely not null in practice but won't enforce. Default to
    # None in case they aren't both required?
    id: Optional[int] = None
    name: Optional[str] = None


class HighlightSchema(BaseModel, extra="forbid", strict=True):
    """
    Validate 'highlights' fields output by the Readwise 'Highlight EXPORT' endpoint.

    Notes
    -----
    - The documentation states 'text' is "technically the only field required" for a
      highlight. However, ``id`` and ``book_id`` are assumed to be required in practice
      and are enforced by the schema.
    - Values defined by the schema and commented 'undocumented' were observed in user
      data.
    """

    id: int = Field(gt=0)  # Undocumented. Guess negatives disallowed.
    text: str = Field(max_length=8191)
    location: Optional[int]  # Documented but negatives not specified. For 'offset'??
    location_type: Optional[str] = Field(
        pattern="^(page|order|time_offset|location|offset|none)$"
    )  # 'location', 'offset' and 'none'(as a str) are undocumented. Legacy?
    note: Optional[str] = Field(max_length=8191)
    color: Optional[str] = Field(
        pattern="^(yellow|blue|pink|orange|green|purple)?$"
    )  # '' not documented.
    highlighted_at: Optional[datetime] = Field(strict=False)
    created_at: Optional[datetime] = Field(strict=False)
    updated_at: Optional[datetime] = Field(strict=False)
    external_id: Optional[str]  # Only example in docs: '6320b2bd7fbcdd7b0c000b3e'
    end_location: None  # Only example in docs. What is allowed?
    url: Optional[str] = Field(max_length=4095)  # Mirror Readwise. No URL checks.
    book_id: int = Field(gt=0)  # See 'user_book_id'.
    # Undocumented. Assume tags will be strings. Nulls possible, not seen in user data
    # and handled by @field_validator. Pydantic accepts empty lists by default.
    is_favorite: Optional[bool]
    is_discard: Optional[bool]
    readwise_url: Optional[str]

    tags: Optional[list[HighlightTagsSchema]]

    @field_validator("tags", mode="before")
    @classmethod
    def replace_null_with_empty_list(cls: BaseModel, value: Optional[list]) -> list:
        """
        Replace a null value with an empty list.

        Parameters
        ----------
        cls: BaseModel
            A Pydantic Schema that inherits from ``Pydantic.BaseModel``

        Returns
        -------
        list
            The passed value if it's a list, or an empty list.

        """
        return value if value else []


class BookSchema(BaseModel, extra="forbid", strict=True):
    """
    Validate books output by the Readwise 'Highlight EXPORT' endpoint.
    """

    user_book_id: int = Field(
        gt=0, strict=True
    )  # Undocumented. Assume negative nums will be disallowed.
    title: str = Field(max_length=511)
    author: Optional[str] = Field(max_length=1024)
    readable_title: str = Field(max_length=511)  # Used same as 'title'.
    source: Optional[str] = Field(min_length=3, max_length=64)  # Used 'source_type'.
    cover_image_url: Optional[str] = Field(max_length=2047)  # Mirror RW. No URL checks.
    unique_url: Optional[str]  # Mirror RW. No URL checks.
    summary: Optional[str]
    # Undocumented. Assume book_tags will be strings. Nulls possible, not seen in
    # user data and handled @field_validator.
    book_tags: list[str]
    category: str = Field(pattern="^(books|articles|tweets|podcasts)$")
    # Undocumented but user data is always null. Docs use "" in examples.
    document_note: Optional[str]
    readwise_url: str  # Mirror RW. No URL checks.
    source_url: Optional[str] = Field(max_length=2047)  # Mirror RW. No URL checks.
    asin: Optional[str] = Field(
        min_length=10, max_length=10, pattern="^[A-Z0-9]{10}$"
    )  # Used Amazon Standard Identification Number.

    highlights: list[HighlightSchema]

    @field_validator("book_tags", mode="before")
    @classmethod
    def replace_null_with_empty_list(cls: BaseModel, value: Optional[list]) -> list:
        """
        See duplicate method on HighlightSchema.
        """
        return value if value else []


if __name__ == "__main__":
    # Placeholder for development testing.
    with open("tests/data/real/sample_all_24th_nov_1604.json", "r") as file_handle:
        data = json.load(file_handle)
    count = 0
    gather = []
    for book in data:
        try:
            pydantic_book = BookSchema(**book)
            count += 1
        except ValueError as err:
            print(f"🚨 Validation error {err} for {book['title']}")
            book.pop("highlights")
            print(book)
    print(gather)
    print(f"Validated {count} books")
