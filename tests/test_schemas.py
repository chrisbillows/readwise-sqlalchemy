from datetime import datetime
from typing import Any, Union

import pytest
from pydantic import ValidationError

from readwise_sqlalchemy.schemas import BookSchema, HighlightSchema

# Mutate dictionary values in place with ``change_nested_dict_value``.
PATH_TO_OBJ = {
    "book": [],
    "highlight": ["highlights", 0],
    "highlight_tag": ["highlights", 0, "tags", 0],
}


# ----------------
# Helper Functions
# ----------------


def mock_api_response():
    """
    Mock a Readwise 'Highlight EXPORT' endpoint ``response.json()["results"]`` output.

    Output contains one book with one highlight. Use a function rather than a constant
    to ensure test isolation.

    Returns
    -------
    list[dict]
        A list of dictionaries where each dictionary is a Readwise book with highlights.
    """
    return [
        {
            "user_book_id": 12345,
            "title": "book title",
            "author": "name surname",
            "readable_title": "Book Title",
            "source": "web_clipper",
            "cover_image_url": "https://link/to/image",
            "unique_url": "http://the.source.url.ai",
            "summary": None,
            "book_tags": ["arch_btw"],
            "category": "books",
            "document_note": "A note added in Readwise Reader",
            "readwise_url": "https://readwise.io/bookreview/12345",
            "source_url": "http://the.source.url.ai",
            "asin": None,
            "highlights": [
                {
                    "id": 10,
                    "text": "The highlight text",
                    "location": 1000,
                    "location_type": "location",
                    "note": "document note",
                    "color": "yellow",
                    "highlighted_at": "2025-01-01T00:01:00",
                    "created_at": "2025-01-01T00:01:10",
                    "updated_at": "2025-01-01T00:01:20",
                    "external_id": None,
                    "end_location": None,
                    "url": None,
                    "book_id": 12345,
                    "tags": [{"id": 97654, "name": "favorite"}],
                    "is_favorite": False,
                    "is_discard": True,
                    "readwise_url": "https://readwise.io/open/10",
                }
            ],
        },
    ]


def change_nested_dict_value(
    base_dict: dict, path: list[Union[str, int]], field: str, value: Any
) -> dict:
    """
    Update a nested dictionary by following a path and setting a field to a new value.

    Parameters
    ----------
    base_dict : dict
        The dictionary to mutate.
    path : list of str or int
        Keys and/or indexes to follow to reach the target dict.
    field : str
        Field to update in the target dict.
    value : Any
        New value to assign.

    Returns
    -------
    dict
        The modified base_dict (mutated in place).
    """
    target = base_dict
    for key in path:
        target = target[key]
    target[field] = value
    return base_dict


def expected_types_per_field() -> dict:
    """
    A dictionary grouping fields by expected type and by object.

    Used for dynamically generating test cases. Use a function rather than a constant
    to ensure test isolation.

    Returns
    -------
    dict[str, dict[str, list]]
        Nested dictionary in the form ``{"object_name": {"type": [field, field ...]}}``.
    """
    return {
        "book": {
            "string": [
                "title",
                "author",
                "readable_title",
                "source",
                "summary",
                "document_note",
                "cover_image_url",
                "readwise_url",
                "source_url",
                "unique_url",
            ],
            "int": ["user_book_id"],
            "list_of_strings": ["book_tags"],
            "choice_category": ["category"],
            "asin": ["asin"],
            "list_of_highlights": ["highlights"],
        },
        "highlight": {
            "string": [
                "text",
                "location_type",
                "note",
                "external_id",
                "url",
                "readwise_url",
            ],
            "int": ["id", "location", "end_location", "book_id"],
            "bool": ["is_favorite", "is_discard"],
            "iso_string": ["highlighted_at", "created_at", "updated_at"],
            "choice_color": ["color"],
            "list_of_highlight_tags": ["tags"],
        },
        "highlight_tag": {"int": ["id"], "string": ["name"]},
    }


def generate_invalid_values_test_cases() -> list[tuple[str, Union[str, int], str]]:
    """
    Generate parametrized test cases to check the configuration of invalid values.

    Returns
    -------
    list[tuple[str, Union[str, int], str]]
        A list of tuples of

        (field, path_to_dict, invalid_value)

    """
    invalid_values = {
        "string": [123, [123], [], ["a"], {}],
        "int": ["a", "123", [], ["a", "b"], {}],
        "list_of_strings": [[1, 2], "a"],
        "choice_category": [],
        "asin": ["a", 1, "1a2b3c4d"],
        "choice_color": [123, [123], [], ["a"], {}],
        "iso_string": [],
        "bool": [0, 1, "a", [], ["a"], {}],
        "list_of_highlight_tags": [123, "abc", [{"a": 1, "b": 2}]],
        "list_of_highlights": [123, "abc", [{"a": 1, "b": 2}]],
    }

    test_cases = []
    for obj, field_group in expected_types_per_field().items():
        for expected_type, fields in field_group.items():
            for field in fields:
                for invalid_value in invalid_values[expected_type]:
                    test_cases.append((field, PATH_TO_OBJ[obj], invalid_value))

    return test_cases


def generate_field_nullability_test_cases() -> dict[list[str]]:
    """
    Generate parametrized test cases to check field nullability configurations.

    Returns
    -------
    dict[list[str]]
         A dictionary with the form: ``{"object_name": ["non_nullable_field",
         "non_nullable_field" ...]}``.
    """
    non_nullable_fields = {
        "book": [
            "user_book_id",
            "title",
            "readable_title",
            "category",
            "readwise_url",
            "highlights",
        ],
        "highlight": ["id", "text", "book_id"],
        "highlight_tag": [],
    }

    mock_book = mock_api_response()[0]
    object_fields = {
        "book": mock_book.keys(),
        "highlight": mock_book["highlights"][0].keys(),
        "highlight_tag": mock_book["highlights"][0]["tags"][0].keys(),
    }

    nullable_test_cases = {"pass": [], "error": []}
    for obj, fields in object_fields.items():
        for field in fields:
            if field in non_nullable_fields[obj]:
                nullable_test_cases["error"].append((field, PATH_TO_OBJ[obj]))
            else:
                nullable_test_cases["pass"].append((field, PATH_TO_OBJ[obj]))
    return nullable_test_cases


# --------------------------
# Tests for Helper Functions
# --------------------------


def test_change_nested_dict_value():
    test_dict = {"k1": [{"k2": [{}, {"k3": "value"}]}]}
    path_to_v2 = ["k1", 0, "k2", 1]
    change_nested_dict_value(test_dict, path_to_v2, "k3", "changed_value")
    assert test_dict == {"k1": [{"k2": [{}, {"k3": "changed_value"}]}]}


def test_book_fields_in_test_objects_match():
    book_fields_mock_api_response = list(mock_api_response()[0].keys())

    book_fields_expected_types_dict = []
    for list_of_fields in expected_types_per_field()["book"].values():
        book_fields_expected_types_dict.extend(list_of_fields)

    assert sorted(book_fields_mock_api_response) == sorted(
        book_fields_expected_types_dict
    )


def test_highlight_fields_in_test_objects_match():
    highlight_fields_mock_api_response = list(
        mock_api_response()[0]["highlights"][0].keys()
    )

    highlight_fields_expected_types_dict = []
    for list_of_fields in expected_types_per_field()["highlight"].values():
        highlight_fields_expected_types_dict.extend(list_of_fields)

    assert sorted(highlight_fields_mock_api_response) == sorted(
        highlight_fields_expected_types_dict
    )


def test_highlight_tags_fields_in_test_objects_match():
    highlight_tag_fields_mock_api_response = mock_api_response()[0]["highlights"][0][
        "tags"
    ][0].keys()

    highlight_tag_fields_expected_types_dict = []
    for list_of_fields in expected_types_per_field()["highlight_tag"].values():
        highlight_tag_fields_expected_types_dict.extend(list_of_fields)

    assert sorted(highlight_tag_fields_mock_api_response) == sorted(
        highlight_tag_fields_expected_types_dict
    )


def test_generate_invalid_values_test_cases():
    test_cases = generate_invalid_values_test_cases()
    assert test_cases[0] == ("title", [], 123)


def test_generate_field_nullability_test_cases():
    test_cases = generate_field_nullability_test_cases()
    assert list(test_cases.keys()) == ["pass", "error"]
    assert test_cases["pass"][0] == ("author", [])
    assert test_cases["error"][0] == ("user_book_id", [])


# -----
# Tests
# -----


def test_valid_values_nested_schema_configuration():
    mock_book_with_hl_and_hl_tag = mock_api_response()[0]
    assert BookSchema(**mock_book_with_hl_and_hl_tag)


def test_valid_values_nested_schema_model_dump_output():
    mock_book_with_hl_and_hl_tag = mock_api_response()[0]
    book_schema = BookSchema(**mock_book_with_hl_and_hl_tag)
    model_dump = book_schema.model_dump()
    expected = {
        "user_book_id": 12345,
        "title": "book title",
        "author": "name surname",
        "readable_title": "Book Title",
        "source": "web_clipper",
        "cover_image_url": "https://link/to/image",
        "unique_url": "http://the.source.url.ai",
        "summary": None,
        "book_tags": ["arch_btw"],
        "category": "books",
        "document_note": "A note added in Readwise Reader",
        "readwise_url": "https://readwise.io/bookreview/12345",
        "source_url": "http://the.source.url.ai",
        "asin": None,
        "highlights": [
            {
                "id": 10,
                "text": "The highlight text",
                "location": 1000,
                "location_type": "location",
                "note": "document note",
                "color": "yellow",
                "highlighted_at": datetime(2025, 1, 1, 0, 1),
                "created_at": datetime(2025, 1, 1, 0, 1, 10),
                "updated_at": datetime(2025, 1, 1, 0, 1, 20),
                "external_id": None,
                "end_location": None,
                "url": None,
                "book_id": 12345,
                "is_favorite": False,
                "is_discard": True,
                "readwise_url": "https://readwise.io/open/10",
                "tags": [{"id": 97654, "name": "favorite"}],
            }
        ],
    }
    assert model_dump == expected


@pytest.mark.parametrize(
    "target_field, path_to_dict, invalid_value", generate_invalid_values_test_cases()
)
def test_invalid_values_nested_schema_configuration(
    target_field: str, path_to_dict: list[Union[str, int]], invalid_value: Any
):
    valid_mock_book_with_hl_and_hl_tag = mock_api_response()[0]
    change_nested_dict_value(
        valid_mock_book_with_hl_and_hl_tag, path_to_dict, target_field, invalid_value
    )
    with pytest.raises(ValidationError):
        BookSchema(**valid_mock_book_with_hl_and_hl_tag)


@pytest.mark.parametrize(
    "field_to_null, path_to_dict", generate_field_nullability_test_cases()["pass"]
)
def test_fields_allow_null(field_to_null: str, path_to_dict: list[Union[str, int]]):
    valid_mock_book_with_hl_and_hl_tag = mock_api_response()[0]
    change_nested_dict_value(
        valid_mock_book_with_hl_and_hl_tag, path_to_dict, field_to_null, None
    )
    assert BookSchema(**valid_mock_book_with_hl_and_hl_tag)


@pytest.mark.parametrize(
    "field_to_null, path_to_dict", generate_field_nullability_test_cases()["error"]
)
def test_fields_error_for_null(field_to_null: str, path_to_dict: list[Union[str, int]]):
    valid_mock_book_with_hl_and_hl_tag = mock_api_response()[0]
    change_nested_dict_value(
        valid_mock_book_with_hl_and_hl_tag, path_to_dict, field_to_null, None
    )
    with pytest.raises(ValidationError):
        BookSchema(**valid_mock_book_with_hl_and_hl_tag)


@pytest.mark.parametrize("field_to_remove", mock_api_response()[0].keys())
def test_missing_book_fields_raise_errors(field_to_remove: str):
    mock_book = mock_api_response()[0]
    del mock_book[field_to_remove]
    with pytest.raises(ValidationError):
        BookSchema(**mock_book)


@pytest.mark.parametrize(
    "field_to_remove", mock_api_response()[0]["highlights"][0].keys()
)
def test_missing_highlight_fields_raise_errors(field_to_remove: str):
    mock_book = mock_api_response()[0]
    del mock_book["highlights"][0][field_to_remove]
    with pytest.raises(ValidationError):
        BookSchema(**mock_book)


@pytest.mark.parametrize(
    "field_to_remove", mock_api_response()[0]["highlights"][0]["tags"][0].keys()
)
def test_missing_highlight_tag_fields_do_not_raise_errors(field_to_remove: str):
    mock_book = mock_api_response()[0]
    del mock_book["highlights"][0]["tags"][0][field_to_remove]
    BookSchema(**mock_book)


def test_additional_book_field_raises_error():
    mock_book = mock_api_response()[0]
    mock_book["extra_field"] = None
    with pytest.raises(ValidationError):
        BookSchema(**mock_book)


def test_additional_highlight_field_raises_error():
    mock_book = mock_api_response()[0]
    mock_book["highlights"][0]["extra_field"] = None
    with pytest.raises(ValidationError):
        BookSchema(**mock_book)


def test_additional_highlight_tag_field_does_not_raise_error():
    mock_book = mock_api_response()[0]
    mock_book["highlights"][0]["tags"][0]["extra_field"] = None
    with pytest.raises(ValidationError):
        BookSchema(**mock_book)


def test_book_field_validator_replace_null_with_empty_list():
    mock_book = mock_api_response()[0]
    mock_book["book_tags"] = None
    book_schema = BookSchema(**mock_book)
    assert book_schema.book_tags == []


def test_highlight_field_validator_replace_null_with_empty_list():
    mock_book = mock_api_response()[0]
    mock_highlight = mock_book["highlights"][0]
    mock_highlight["tags"] = None
    highlight_schema = HighlightSchema(**mock_highlight)
    assert highlight_schema.tags == []
