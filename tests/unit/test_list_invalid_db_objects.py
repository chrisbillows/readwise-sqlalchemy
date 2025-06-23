import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from readwise_sqlalchemy.list_invalid_db_objects import list_invalid_db_objects
from readwise_sqlalchemy.models import (
    Base,
    Book,
    BookTag,
    Highlight,
    HighlightTag,
    ReadwiseBatch,
)
from tests.helpers import flat_mock_api_response_fully_validated


def test_list_invalid_db_objects(capsys: pytest.CaptureFixture[str]):
    orm_models = [Book, BookTag, Highlight, HighlightTag]
    test_objects = flat_mock_api_response_fully_validated()
    test_objects = [obj[0] for obj in test_objects.values()]
    for obj in test_objects:
        obj["validated"] = False
        obj["validation_errors"].update({"mock_field": "mock_error"})

    ANY_TIME = datetime(2025, 1, 1, 10, 10, 10)
    batch = ReadwiseBatch(start_time=ANY_TIME, end_time=ANY_TIME)
    orm_models = [
        model(**obj, batch=batch) for model, obj in zip(orm_models, test_objects)
    ]

    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test.db"
        engine = create_engine(f"sqlite:///{db_path}")
        Session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)

        with Session() as session:
            session.add(batch)
            session.flush()
            session.add_all(orm_models)
            batch.database_write_time = ANY_TIME
            session.commit()

            mock_user_config = Mock()
            mock_user_config.db_path = db_path

            list_invalid_db_objects(mock_user_config)

            captured = capsys.readouterr()
            actual = captured.out

            # These are the actual instances but we're only using their string
            # representation.
            expected = (
                f"4 invalid objects found:\n"
                f"[Book] {orm_models[0]}\n"
                f"  - mock_field: mock_error\n"
                f"\n"
                f"[BookTag] {orm_models[1]}\n"
                f"  - mock_field: mock_error\n"
                f"\n"
                f"[Highlight] {orm_models[2]}\n"
                f"  - mock_field: mock_error\n"
                f"\n"
                f"[HighlightTag] {orm_models[3]}\n"
                f"  - mock_field: mock_error\n"
                f"\n"
            )
            assert actual == expected
