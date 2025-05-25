from datetime import datetime
from typing import Any, Union

import pytest
from pydantic import ValidationError

from readwise_sqlalchemy.main import SCHEMAS_BY_OBJECT
from readwise_sqlalchemy.schemas import BookSchemaUnnested, HighlightSchemaUnnested
from tests.helpers import flat_mock_api_response_nested_validated, mock_api_response

# Mutate dictionary values in place with ``change_nested_dict_value()``.
PATH_TO_OBJ = {
    "book": [],
    "book_tag": ["book_tags", 0],
    "highlight": ["highlights", 0],
    "highlight_tag": ["highlights", 0, "tags", 0],
}


# --------
# Fixtures
# --------


@pytest.fixture
def flat_objects_api_fields_only() -> dict[str, list[dict[str, Any]]]:
    """
    Extract only the API fields from an flattened API response that is nested validated.

    Pydantic validation is only carried out on the API fields. Replicate that content
    for testing pydantic schema.

    Returns
    -------
    dict[str, list[dict[str, Any]]]
        A dictionary where keys are object types and values are lists of objects: one
        object per object type.
    """
    flattened_nested_validated_data = flat_mock_api_response_nested_validated()
    objs_with_only_api_fields = {}
    for obj_type, objs in flattened_nested_validated_data.items():
        obj_schema = SCHEMAS_BY_OBJECT[obj_type]
        for obj in objs:
            obj_with_api_fields_only = {
                k: v for k, v in obj.items() if k in obj_schema.model_fields.keys()
            }
            objs_with_only_api_fields[obj_type] = obj_with_api_fields_only
    return objs_with_only_api_fields


# ----------------
# Helper Functions
# ----------------


def change_nested_dict_value(
    base_dict: dict, path: list[Union[str, int]], field: str, value: Any
) -> dict:
    """
    Update a value in a nested dictionary in place by following a given path.

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


def expected_type_per_schema_field() -> dict[str, dict[str, list[str]]]:
    """
    A dictionary grouping schema fields by expected type and by schema object.

    Used for dynamically generating test cases. Use a function rather than a constant
    to ensure test isolation.

    Returns
    -------
    dict[str, dict[str, list[str]]]
        Nested dictionary in the form ``{"object_name": {"expected_type": [field,
        field ...]}}``.
    """
    return {
        "books": {
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
            "list_of_tags": ["book_tags"],
            "choice_category": ["category"],
            "asin": ["asin"],
            "list_of_highlights": ["highlights"],
            "bool": ["is_deleted"],
        },
        "highlights": {
            "string": [
                "text",
                "location_type",
                "note",
                "external_id",
                "url",
                "readwise_url",
            ],
            "int": ["id", "location", "end_location", "book_id"],
            "bool": ["is_favorite", "is_discard", "is_deleted"],
            "iso_string": ["highlighted_at", "created_at", "updated_at"],
            "choice_color": ["color"],
            "list_of_tags": ["tags"],
        },
        "book_tags": {"int": ["id"], "string": ["name"]},
        "highlight_tags": {"int": ["id"], "string": ["name"]},
    }


def generate_invalid_field_values_test_cases() -> list[
    tuple[str, Union[str, int], str]
]:
    """
    Generate parametrized test cases to check the configuration of invalid values.

    Returns
    -------
    list[tuple[str, Union[str, int], str]]
        A list of test cases where each test case is a tuple in the form ``(obj, field,
        invalid_value)``.
    """
    invalid_values = {
        "string": [123, [123], [], ["a"], {}],
        "int": ["a", "123", [], ["a", "b"], {}],
        "choice_category": [],
        "asin": ["a", 1, "1a2b3c4d"],
        "choice_color": [123, [123], [], ["a"], {}],
        "iso_string": [],
        "bool": [0, 1, "a", [], ["a"], {}],
        "list_of_tags": [123, "abc", [{"a": 1, "b": 2}]],
        "list_of_highlights": [123, "abc", [{"a": 1, "b": 2}]],
    }

    test_cases = []
    for obj, field_group in expected_type_per_schema_field().items():
        for expected_type, fields in field_group.items():
            for field in fields:
                for invalid_value in invalid_values[expected_type]:
                    test_cases.append((obj, field, invalid_value))
    return test_cases


def generate_field_nullability_test_cases() -> dict[str, list[tuple]]:
    """
    Generate parametrized test cases to check field nullability configurations.

    Returns
    -------
    dict[str[list[tuple]]]
        A dictionary with the keys ``error`` and ``pass``. The values for each are a
        list of test cases in the form ``(obj, field)``.
    """
    non_nullable_fields = {
        "books": [
            "user_book_id",
            "title",
            "readable_title",
            "category",
            "readwise_url",
            "highlights",
        ],
        "highlights": ["id", "text", "book_id"],
        "highlight_tags": [],
        "book_tags": [],
    }
    nullable_test_cases = {"pass": [], "error": []}
    for obj, schema in SCHEMAS_BY_OBJECT.items():
        for field in schema.model_fields.keys():
            if field in non_nullable_fields[obj]:
                nullable_test_cases["error"].append((obj, field))
            else:
                nullable_test_cases["pass"].append((obj, field))
    return nullable_test_cases


# --------------------------
# Tests for Helper Functions
# --------------------------


def test_book_fields_in_test_objects_match():
    book_fields_mock_api_response = list(mock_api_response()[0].keys())

    book_fields_expected_types_dict = []
    for list_of_fields in expected_type_per_schema_field()["book"].values():
        book_fields_expected_types_dict.extend(list_of_fields)

    assert sorted(book_fields_mock_api_response) == sorted(
        book_fields_expected_types_dict
    )


def test_book_tag_fields_in_test_objects_match():
    book_tags_fields_mock_api_response = mock_api_response()[0]["book_tags"][0].keys()

    book_tag_fields_expected_types_dict = []
    for list_of_fields in expected_type_per_schema_field()["book_tag"].values():
        book_tag_fields_expected_types_dict.extend(list_of_fields)

    assert sorted(book_tags_fields_mock_api_response) == sorted(
        book_tag_fields_expected_types_dict
    )


def test_highlight_fields_in_test_objects_match():
    highlight_fields_mock_api_response = list(
        mock_api_response()[0]["highlights"][0].keys()
    )

    highlight_fields_expected_types_dict = []
    for list_of_fields in expected_type_per_schema_field()["highlight"].values():
        highlight_fields_expected_types_dict.extend(list_of_fields)

    assert sorted(highlight_fields_mock_api_response) == sorted(
        highlight_fields_expected_types_dict
    )


def test_highlight_tags_fields_in_test_objects_match():
    highlight_tag_fields_mock_api_response = mock_api_response()[0]["highlights"][0][
        "tags"
    ][0].keys()

    highlight_tag_fields_expected_types_dict = []
    for list_of_fields in expected_type_per_schema_field()["highlight_tag"].values():
        highlight_tag_fields_expected_types_dict.extend(list_of_fields)

    assert sorted(highlight_tag_fields_mock_api_response) == sorted(
        highlight_tag_fields_expected_types_dict
    )


def test_change_nested_dict_value():
    test_dict = {"k1": [{"k2": [{}, {"k3": "value"}]}]}
    path_to_v2 = ["k1", 0, "k2", 1]
    change_nested_dict_value(test_dict, path_to_v2, "k3", "changed_value")
    assert test_dict == {"k1": [{"k2": [{}, {"k3": "changed_value"}]}]}


def test_generate_invalid_field_values_test_cases():
    test_cases = generate_invalid_field_values_test_cases()
    assert test_cases[0] == ("title", [], 123)


def test_generate_field_nullability_test_cases():
    test_cases = generate_field_nullability_test_cases()
    assert list(test_cases.keys()) == ["pass", "error"]
    assert test_cases["pass"][0] == ("is_deleted", [])
    assert test_cases["pass"][1] == ("author", [])
    assert test_cases["error"][0] == ("user_book_id", [])


# -----
# Tests
# -----


@pytest.mark.parametrize(
    "object_type", ["books", "book_tags", "highlights", "highlight_tags"]
)
def test_flat_schema_configuration_by_object(
    flat_objects_api_fields_only: dict[str, list[dict[str, Any]]], object_type: str
):
    schema = SCHEMAS_BY_OBJECT[object_type]
    test_object = flat_objects_api_fields_only[object_type]
    assert schema(**test_object)


@pytest.mark.parametrize(
    "object_type, expected",
    [
        (
            "books",
            {
                "user_book_id": 12345,
                "title": "book title",
                "is_deleted": False,
                "author": "name surname",
                "readable_title": "Book Title",
                "source": "web_clipper",
                "cover_image_url": "https://link/to/image",
                "unique_url": "http://the.source.url.ai",
                "summary": None,
                "category": "books",
                "document_note": "A note added in Readwise Reader",
                "readwise_url": "https://readwise.io/bookreview/12345",
                "source_url": "http://the.source.url.ai",
                "asin": None,
            },
        ),
        ("book_tags", {"id": 6969, "name": "arch_btw"}),
        (
            "highlights",
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
                "is_deleted": False,
                "readwise_url": "https://readwise.io/open/10",
            },
        ),
        ("highlight_tags", {"id": 97654, "name": "favorite"}),
    ],
)
def test_flat_schema_model_dump_output(
    flat_objects_api_fields_only: dict[str, list[dict[str, Any]]],
    object_type: str,
    expected: dict[str, Any],
):
    schema = SCHEMAS_BY_OBJECT[object_type]
    test_object = flat_objects_api_fields_only[object_type]
    test_object_as_schema = schema(**test_object)
    model_dump = test_object_as_schema.model_dump()
    assert model_dump == expected


@pytest.mark.parametrize(
    "object_type, target_field, invalid_value",
    generate_invalid_field_values_test_cases(),
)
def test_flat_schema_configuration_with_invalid_values(
    object_type: str,
    target_field: str,
    invalid_value: Any,
    flat_objects_api_fields_only: dict,
):
    object_under_test = flat_objects_api_fields_only[object_type]
    object_under_test[target_field] = invalid_value
    schema = SCHEMAS_BY_OBJECT[object_type]
    with pytest.raises(ValidationError):
        schema(**object_under_test)


@pytest.mark.parametrize(
    "object_type, field_to_null", generate_field_nullability_test_cases()["pass"]
)
def test_flat_schema_configuration_fields_allow_null(
    object_type: str,
    field_to_null: str,
    flat_objects_api_fields_only: dict,
):
    object_under_test = flat_objects_api_fields_only[object_type]
    object_under_test[field_to_null] = None
    schema = SCHEMAS_BY_OBJECT[object_type]
    assert schema(**object_under_test)


@pytest.mark.parametrize(
    "object_type, field_to_null", generate_field_nullability_test_cases()["error"]
)
def test_flat_schema_configuration_fields_error_for_null(
    object_type: str,
    field_to_null: str,
    flat_objects_api_fields_only: dict,
):
    object_under_test = flat_objects_api_fields_only[object_type]
    object_under_test[field_to_null] = None
    schema = SCHEMAS_BY_OBJECT[object_type]
    with pytest.raises(ValidationError):
        schema(**object_under_test)


@pytest.mark.parametrize("field_to_remove", mock_api_response()[0].keys())
def test_missing_book_fields_raise_errors(field_to_remove: str):
    mock_book = mock_api_response()[0]
    del mock_book[field_to_remove]
    with pytest.raises(ValidationError):
        BookSchemaUnnested(**mock_book)


@pytest.mark.parametrize(
    "field_to_remove", mock_api_response()[0]["highlights"][0].keys()
)
def test_missing_highlight_fields_raise_errors(field_to_remove: str):
    mock_book = mock_api_response()[0]
    del mock_book["highlights"][0][field_to_remove]
    with pytest.raises(ValidationError):
        BookSchemaUnnested(**mock_book)


@pytest.mark.parametrize(
    "field_to_remove", mock_api_response()[0]["book_tags"][0].keys()
)
def test_missing_book_tag_fields_do_not_raise_errors(field_to_remove: str):
    mock_book = mock_api_response()[0]
    del mock_book["book_tags"][0][field_to_remove]
    BookSchemaUnnested(**mock_book)


@pytest.mark.parametrize(
    "field_to_remove",
    mock_api_response()[0]["highlights"][0]["tags"][0].keys(),
)
def test_missing_highlight_tag_fields_do_not_raise_errors(field_to_remove: str):
    mock_book = mock_api_response()[0]
    del mock_book["highlights"][0]["tags"][0][field_to_remove]
    BookSchemaUnnested(**mock_book)


def test_additional_book_field_raises_error():
    mock_book = mock_api_response()[0]
    mock_book["extra_field"] = None
    with pytest.raises(ValidationError):
        BookSchemaUnnested(**mock_book)


def test_additional_book_tag_field_raises_error():
    mock_book = mock_api_response()[0]
    mock_book["book_tags"][0]["extra_field"] = None
    with pytest.raises(ValidationError):
        BookSchemaUnnested(**mock_book)


def test_additional_highlight_field_raises_error():
    mock_book = mock_api_response()[0]
    mock_book["highlights"][0]["extra_field"] = None
    with pytest.raises(ValidationError):
        BookSchemaUnnested(**mock_book)


def test_additional_highlight_tag_field_raises_error():
    mock_book = mock_api_response()[0]
    mock_book["highlights"][0]["tags"][0]["extra_field"] = None
    with pytest.raises(ValidationError):
        BookSchemaUnnested(**mock_book)


def test_book_field_validator_replaces_null_with_an_empty_list():
    mock_book = mock_api_response()[0]
    mock_book["book_tags"] = None
    book_schema = BookSchemaUnnested(**mock_book)
    assert book_schema.book_tags == []


def test_highlight_field_validator_replaces_null_with_an_empty_list():
    mock_book = mock_api_response()[0]
    mock_highlight = mock_book["highlights"][0]
    mock_highlight["tags"] = None
    highlight_schema = HighlightSchemaUnnested(**mock_highlight)
    assert highlight_schema.tags == []
