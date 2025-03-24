from typing import Any, Union

import pytest
from pydantic import ValidationError

from readwise_sqlalchemy.schemas import BookSchema


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
                    "note": "",
                    "color": "yellow",
                    "highlighted_at": "2025-01-01T00:01:00",
                    "created_at": "2025-01-01T00:01:10",
                    "updated_at": "2025-01-01T00:01:20",
                    "external_id": None,
                    "end_location": None,
                    "url": None,
                    "book_id": 12345,
                    "tags": [{"id": 97654, "name": "favourite"}],
                    "is_favorite": False,
                    "is_discard": False,
                    "readwise_url": "https://readwise.io/open/10",
                }
            ],
        },
    ]


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


def test_nested_schemas_configuration_with_valid_values():
    mock_book_with_hl_and_hl_tag = mock_api_response()[0]
    assert BookSchema(**mock_book_with_hl_and_hl_tag)


def test_nested_schemas_configuration_model_dump_output():
    pass


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


def generate_invalid_values_test_cases() -> list[tuple[str, Union[str, int], str]]:
    """
    Generate parametrized test cases for invalid values.

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

    path_to_dict = {
        "book": [],
        "highlight": [],
        "highlight_tag": ["highlights", 0, "tags", 0],
    }

    test_cases = []
    for obj, field_group in expected_types_per_field().items():
        for expected_type, fields in field_group.items():
            for field in fields:
                for invalid_value in invalid_values[expected_type]:
                    test_cases.append((field, path_to_dict[obj], invalid_value))

    return test_cases


@pytest.mark.parametrize(
    "target_field, path_to_dict, invalid_value", generate_invalid_values_test_cases()
)
def test_nested_schema_configuration_with_invalid_values_for_book_fields(
    target_field: str, path_to_dict: str, invalid_value: Any
):
    mock_book_with_hl_and_hl_tag = mock_api_response()[0]
    change_nested_dict_value(
        mock_book_with_hl_and_hl_tag, path_to_dict, target_field, invalid_value
    )
    with pytest.raises(ValidationError):
        BookSchema(**mock_book_with_hl_and_hl_tag)


NON_NULLABLE_FIELDS = {
    "book": [
        "user_book_id",
        "title",
        "readable_title",
        "category",
        "readwise_url",
        "highlights",
    ],
    "highlight": ["id", "text", "book_id"],
    "highlight_tag": None,
}


def test_nested_schema_configuration_for_null_fields():
    pass


# # Raise if any field is missing.
# @pytest.mark.parametrize(
#     "removed_field", [field for field in HIGHLIGHT_TAGS_SCHEMA_VARIANTS.keys()]
# )
# def test_highlight_tags_schema_with_missing_fields(
#     removed_field: str, mock_highlight_tags: dict[str, Any]
# ):
#     del mock_highlight_tags[removed_field]
#     # Missing fields are intended to be allowed.
#     HighlightTagsSchema(**mock_highlight_tags)


# def test_highlight_tags_schema_config_with_unexpected_field(mock_highlight_tags: dict):
#     mock_highlight_tags["extra_field"] = None
#     with pytest.raises(ValidationError):
#         HighlightTagsSchema(**mock_highlight_tags)


# def test_highlight_schema_with_valid_values(mock_highlight: dict):
#     assert HighlightSchema(**mock_highlight)


# @pytest.mark.parametrize("invalid_field", HIGHLIGHT_SCHEMA_VARIANTS.keys())
# def test_highlight_schema_with_invalid_types(
#     invalid_field: str, mock_highlight: dict[str, Any]
# ):
#     mock_highlight[invalid_field] = HIGHLIGHT_SCHEMA_VARIANTS[invalid_field][
#         "value_invalid_type"
#     ]
#     with pytest.raises(ValidationError):
#         HighlightSchema(**mock_highlight)


# @pytest.mark.parametrize(
#     "valid_null_field",
#     [
#         field
#         for field in HIGHLIGHT_SCHEMA_VARIANTS.keys()
#         if HIGHLIGHT_SCHEMA_VARIANTS[field]["nullable"]
#     ],
# )
# def test_highlight_schema_with_null_values_where_allowed(
#     valid_null_field: str, mock_highlight: dict[str, Any]
# ):
#     mock_highlight[valid_null_field] = None
#     assert HighlightSchema(**mock_highlight)


# @pytest.mark.parametrize(
#     "invalid_null_field",
#     [
#         field
#         for field in HIGHLIGHT_SCHEMA_VARIANTS.keys()
#         if not HIGHLIGHT_SCHEMA_VARIANTS[field]["nullable"]
#     ],
# )
# def test_highlight_schema_with_null_values_where_not_allowed(
#     invalid_null_field: str, mock_highlight: dict[str, Any]
# ):
#     mock_highlight[invalid_null_field] = None
#     with pytest.raises(ValidationError):
#         HighlightSchema(**mock_highlight)


# # Raise if any field is missing.
# @pytest.mark.parametrize(
#     "removed_field", [field for field in HIGHLIGHT_SCHEMA_VARIANTS.keys()]
# )
# def test_highlight_schema_with_missing_fields(
#     removed_field: str, mock_highlight: dict[str, Any]
# ):
#     del mock_highlight[removed_field]
#     with pytest.raises(ValidationError):
#         HighlightSchema(**mock_highlight)


# def test_highlight_schema_config_with_unexpected_field(mock_highlight: dict):
#     mock_highlight["extra_field"] = None
#     with pytest.raises(ValidationError):
#         HighlightSchema(**mock_highlight)


# def test_highlight_replace_null_with_empty_list_for_tags(mock_highlight: dict):
#     mock_highlight["tags"] = None
#     highlight = HighlightSchema(**mock_highlight)
#     assert highlight.tags == []


# def test_book_schema_with_valid_values(mock_book: dict):
#     assert BookSchema(**mock_book)


# @pytest.mark.parametrize("invalid_field", BOOK_SCHEMA_VARIANTS.keys())
# def test_book_schema_with_invalid_types(invalid_field: str, mock_book: dict[str, Any]):
#     mock_book[invalid_field] = BOOK_SCHEMA_VARIANTS[invalid_field]["value_invalid_type"]
#     with pytest.raises(ValidationError):
#         BookSchema(**mock_book)


# @pytest.mark.parametrize(
#     "valid_null_field",
#     [
#         field
#         for field in BOOK_SCHEMA_VARIANTS.keys()
#         if BOOK_SCHEMA_VARIANTS[field]["nullable"]
#     ],
# )
# def test_book_schema_with_null_values_where_allowed(
#     valid_null_field: str, mock_book: dict[str, Any]
# ):
#     mock_book[valid_null_field] = None
#     assert BookSchema(**mock_book)


# @pytest.mark.parametrize(
#     "invalid_null_field",
#     [
#         field
#         for field in BOOK_SCHEMA_VARIANTS.keys()
#         if not BOOK_SCHEMA_VARIANTS[field]["nullable"]
#     ],
# )
# def test_book_schema_with_null_values_where_not_allowed(
#     invalid_null_field: str, mock_book: dict[str, Any]
# ):
#     mock_book[invalid_null_field] = None
#     with pytest.raises(ValidationError):
#         BookSchema(**mock_book)


# # Raise if any field is missing.
# @pytest.mark.parametrize(
#     "removed_field", [field for field in BOOK_SCHEMA_VARIANTS.keys()]
# )
# def test_book_schema_with_missing_fields(removed_field: str, mock_book: dict[str, Any]):
#     del mock_book[removed_field]
#     with pytest.raises(ValidationError):
#         BookSchema(**mock_book)


# def test_book_schema_config_with_unexpected_field(mock_book: dict):
#     mock_book["extra_field"] = None
#     with pytest.raises(ValidationError):
#         HighlightSchema(**mock_book)


# def test_book_replace_null_with_empty_list_for_book_tags(mock_book: dict):
#     mock_book["book_tags"] = None
#     book = BookSchema(**mock_book)
#     assert book.book_tags == []
