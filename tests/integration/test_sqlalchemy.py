def test_validate_and_write_mock_book_to_db(mock_book: dict, mock_highlight: dict):
    mock_book.highlights = [mock_highlight]
