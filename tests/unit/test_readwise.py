from unittest.mock import MagicMock, Mock, patch

from readwise_sqlalchemy.integrations.readwise import fetch_from_export_api


@patch("readwise_sqlalchemy.integrations.readwise.requests")
def test_fetch_from_export_api(mock_requests: MagicMock):
    # Helper to build a mock response object
    def make_mock_response(json_data):
        mock_response = Mock()
        mock_response.json.return_value = json_data
        return mock_response

    # Set side_effect to return these responses on consecutive calls
    mock_requests.get.side_effect = [
        make_mock_response(
            {
                "count": 2,  # Page counter.
                "nextPageCursor": 98765432,  # Seem to be long ints.
                "results": [{"a": 1}, {"b": 2}],
            }
        ),
        make_mock_response(
            {
                "count": 1,
                "nextPageCursor": 87654321,
                "results": [{"c": 3}],
            }
        ),
        make_mock_response(
            {
                "count": 0,
                "nextPageCursor": None,
                "results": [],
            },
        ),
    ]
    last_fetch = "2025-04-14T20:28:21.589651"
    mock_user_config = Mock()
    mock_user_config.READWISE_API_TOKEN = "abc123"

    actual = fetch_from_export_api(last_fetch, mock_user_config)

    assert actual == [{"a": 1}, {"b": 2}, {"c": 3}]
