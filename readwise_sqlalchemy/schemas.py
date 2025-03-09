"""
Pydantic schema for data validation.

Note
----
- Where a schema attribute has a defined ``Field`` - e.g. ``Field(max_length=8191)``
  this indicates there is documentation for the attribute. Many attributes are not
  documented. (See: https://readwise.io/api_deets). Assumptions about attributes are
  noted as inline comments.
- All models pass ``extra=forbid`` which causes unexpected, undefined fields to raise an
  error. Pydantic schema accepted unexpected fields by default.
"""

import json
from datetime import datetime
from typing import Optional

from pydantic import AnyUrl, BaseModel, Field, HttpUrl, field_validator


class HighlightTagsSchema(BaseModel, extra="forbid"):
    """
    Validate 'tags' fields in a HighlightSchema highlight.
    """

    # Undocumented. Seems likely not null in practice but won't enforce. Default to
    # None in case they aren't both required?
    id: Optional[int] = None
    name: Optional[str] = None


class HighlightSchema(BaseModel, extra="forbid"):
    """
    Validate 'highlights' fields output by the Readwise 'Highlight EXPORT' endpoint.

    Notes
    -----
    - The documentation states 'text' is "technically the only field required" for a
      highlight. However, 'id' and 'book_id' are assumed to be required in practice
      which is enforced by the schema.
    - Values defined by the schema and commented 'undocumented' were observed in user
      data.
    """

    id: int = Field(gt=0, strict=True)  # Undocumented. Guess negatives disallowed.
    text: str = Field(max_length=8191)
    location: Optional[int]  # Documented but negatives not specified. For 'offset'??
    location_type: Optional[str] = Field(
        pattern="^(page|order|time_offset|location|offset|none)$"
    )  # 'location', 'offset' and 'none'(as a str!) are undocumented. Legacy?
    note: Optional[str] = Field(max_length=8191)
    color: Optional[str] = Field(
        pattern="^(yellow|blue|pink|orange|green|purple)?$"
    )  # '' not documented.
    highlighted_at: Optional[datetime]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    external_id: Optional[str]  # Only example in docs: '6320b2bd7fbcdd7b0c000b3e'
    end_location: None  # Only example in docs. What is allowed?
    url: Optional[AnyUrl] = Field(max_length=4095)
    book_id: int = Field(gt=0, strict=True)  # See 'user_book_id'.
    # Undocumented. Assume tags will be strings. Nulls possible, not seen in
    # user data and handled @field_validator. Pydantic accepts empty lists by default.
    is_favorite: Optional[bool] = Field(strict=True)
    is_discard: Optional[bool] = Field(strict=True)
    readwise_url: Optional[HttpUrl]

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


class BookSchema(BaseModel, extra="forbid"):
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
    cover_image_url: Optional[HttpUrl] = Field(max_length=2047)  # Used 'image_url'.
    unique_url: Optional[HttpUrl]
    summary: Optional[str]
    # Undocumented. Assume book_tags will be strings. Nulls possible, not seen in
    # user data and handled @field_validator.
    book_tags: list[str]
    category: str = Field(pattern="^(books|articles|tweets|podcasts)$")
    # Undocumented but user data is always null. Docs use "" in examples.
    document_note: Optional[str]
    readwise_url: HttpUrl
    source_url: Optional[AnyUrl] = Field(max_length=2047)
    asin: Optional[str] = Field(
        min_length=10, max_length=10, pattern="^[A-Z0-9]{10}$"
    )  # Used Amazon Standard Identification Number.

    highlights: list[HighlightSchema]

    @field_validator("book_tags", mode="before")
    @classmethod
    def replace_null_with_empty_list(cls: BaseModel, value: Optional[list]) -> list:
        """
        See duplicate method on BookSchema.
        """
        return value if value else []


if __name__ == "__main__":
    # Placeholder for development testing.
    with open("tests/data/real/sample_all_24th_nov_1604.json", "r") as file_handle:
        data = json.load(file_handle)
    count = 0
    highlight_ids = []
    reused_highlight_ids = []
    for book in data:
        try:
            pydantic_book = BookSchema(**book)
            count += 1
            for highlight in book["highlights"]:
                for highlight_tag in highlight["tags"]:
                    if highlight_tag["name"] == "orange":
                        print(highlight_tag)

        except ValueError as err:
            print(f"🚨 Validation error {err} for {book['title']}")
    print(len(highlight_ids))
    print(reused_highlight_ids)
    print(f"Validated {count} books")
