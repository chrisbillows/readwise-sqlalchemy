import pytest

from readwise_sqlalchemy.utils import (
    generate_random_number,
    get_columns_and_values,
)


@pytest.mark.skip("To complete implementation")
def test_get_columns_and_values(minimal_book_as_orm):
    actual = get_columns_and_values(minimal_book_as_orm)
    expected = {
        "user_book_id": 99,
        "title": "book_2",
        "author": None,
        "readable_title": None,
        "source": None,
        "cover_image_url": None,
        "unique_url": None,
        "summary": None,
        "book_tags": [],
        "category": None,
        "document_note": None,
        "readwise_url": None,
        "source_url": None,
        "asin": None,
        "batch_id": 1,
    }
    assert actual == expected


def test_generate_random_number():
    actual = generate_random_number(5)
    assert isinstance(actual, int)
    assert len(str(actual)) == 5


@pytest.mark.skip("To implement")
def test_generate_api_response_data_readwise_highlight_export_endpoint():
    pass
