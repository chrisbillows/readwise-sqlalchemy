import json
from typing import Optional

from pydantic import BaseModel, Field, HttpUrl


class BookSchema(BaseModel):
    user_book_id: int = Field(gt=0)
    title: str = Field(max_length=511)
    author: Optional[str] = Field(max_length=1024)
    # Undocumented. Use documentation for 'title'
    readable_title: str = Field(max_length=511)
    # Undocumented. Use documentation for 'source_type'
    source: Optional[str] = Field(min_length=3, max_length=64)
    cover_image_url: Optional[HttpUrl]


# class HighlightSchema(BaseModel):
#     id: int
#     text: str = Field(..., max_length=8191)
#     location: Optional[int] = None
#     location_type: str = Field("order", regex="^(page|order|time_offset)$")  # Enforce allowed values
#     note:
#     source_url: Optional[HttpUrl] = None
#     source_type: Optional[str] = Field(None, min_length=3, max_length=64)

#     note: Optional[str] = Field(None, max_length=8191)
#     highlighted_at: Optional[datetime] = None
#     highlight_url: Optional[str] = Field(None, max_length=4095)


if __name__ == "__main__":
    with open("tests/data/real/sample_all_24th_nov_1604.json", "r") as file_handle:
        data = json.load(file_handle)

    count = 0
    for book in data:
        try:
            pydantic_book = BookSchema(**book)
            count += 1
            print(pydantic_book.model_dump())
        except ValueError as e:
            print(f"🚨 Validation error {e}")
    print(f"Validated {count} books")
