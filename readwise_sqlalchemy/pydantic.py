import json
from typing import Optional

from pydantic import AnyUrl, BaseModel, Field, HttpUrl


class HighlightSchema(BaseModel):
    pass


#     id: int
#     text: str = Field(..., max_length=8191)
#     location: Optional[int] = None
#     location_type: str = Field("order", regex="^(page|order|time_offset)$")  # Enforce allowed values
#     note: Optional[str] = Field(None, max_length=8191)
#     highlighted_at: Optional[datetime] = None
#     highlight_url: Optional[str] = Field(None, max_length=4095)


class BookSchema(BaseModel):
    """
    Pydantic model schema.  Validate Readwise 'Highlight EXPORT' endpoint.

    Notes
    -----
    A defined 'Field' indicates the key is documented. Most of the keys are not
    documented. See: https://readwise.io/api_deets
    """

    user_book_id: int = Field(gt=0)  # Undocumented. Guess negative nums disallowed.
    title: str = Field(max_length=511)
    author: Optional[str] = Field(max_length=1024)
    readable_title: str = Field(max_length=511)  # Used same as 'title'.
    source: Optional[str] = Field(min_length=3, max_length=64)  # Used 'source_type'.
    cover_image_url: Optional[HttpUrl] = Field(max_length=2047)  # Used 'image_url'.
    unique_url: Optional[HttpUrl]
    summary: Optional[str]
    book_tags: list[Optional[str]]  # Undocumented. Guess tags would be strings.
    category: str = Field(pattern="^(books|articles|tweets|podcasts)$")
    readwise_url: HttpUrl
    source_url: Optional[AnyUrl] = Field(max_length=2047)
    asin: Optional[str] = Field(
        min_length=10, max_length=10, pattern="^[A-Z0-9]{10}$"
    )  # Used Amazon Standard Identification Number.
    highlights: list[Optional[HighlightSchema]]


if __name__ == "__main__":
    with open("tests/data/real/sample_all_24th_nov_1604.json", "r") as file_handle:
        data = json.load(file_handle)

    count = 0
    for book in data:
        try:
            pydantic_book = BookSchema(**book)
            count += 1
        except ValueError as e:
            print(f"🚨 Validation error {e}")
    print(f"Validated {count} books")
