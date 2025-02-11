import json
from datetime import datetime
from typing import Optional

from pydantic import AnyUrl, BaseModel, Field, HttpUrl


class HighlightSchema(BaseModel):
    """
    Pydantic model schema. Validate Highlight Field in 'Highlight EXPORT' endpoint.

    Notes
    -----
    - A defined 'Field' indicates the key is documented. Most keys are not documented.
      See: https://readwise.io/api_deets
    - The documentation states 'text' is 'technically the only field required'. 'id' is
      still assumed to always be present.
    - Values commented as not documented but allowed have been observed in user data.
    """

    id: int = Field(gt=0, strict=True)  # Undocumented. Guess negatives disallowed.
    text: str = Field(max_length=8191)
    location: Optional[int]  # Documented but negatives not specified. For 'offset'??
    location_type: Optional[str] = Field(
        pattern="^(page|order|time_offset|location|offset|none)$"
    )  # 'location', 'offset' and 'none'(as a str!) not documented. Legacy?
    note: Optional[str] = Field(max_length=8191)
    color: Optional[str] = Field(
        pattern="^(yellow|blue|pink|orange|green|purple)?$"
    )  # '' not documented.
    highlighted_at: Optional[datetime]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    external_id: Optional[str]  # Only example in docs: '6320b2bd7fbcdd7b0c000b3e'


#     highlight_url: Optional[str] = Field(None, max_length=4095)


class BookSchema(BaseModel):
    """
    Pydantic model schema.  Validate Readwise 'Highlight EXPORT' endpoint.

    Notes
    -----
    A defined 'Field' indicates the key is documented. Most of the keys are not
    documented. See: https://readwise.io/api_deets
    """

    user_book_id: int = Field(
        gt=0, strict=True
    )  # Undocumented. Guess negative nums disallowed.
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
    # with open("tests/data/real/example_scratch.json", "r") as file_handle:
    #     data = json.load(file_handle)
    with open("tests/data/real/sample_all_24th_nov_1604.json", "r") as file_handle:
        data = json.load(file_handle)
    count = 0
    for book in data:
        try:
            pydantic_book = BookSchema(**book)
            print(f"{[hl_dict['external_id'] for hl_dict in book['highlights']]}")
            count += 1
        except ValueError:
            # print(f"🚨 Validation error {e} for {book['highlights'][0]['location']}")
            print(
                f"Failed for {book['title']} with {[hl_dict['color'] for hl_dict in book['highlights']]}"
            )
    print(f"Validated {count} books")
