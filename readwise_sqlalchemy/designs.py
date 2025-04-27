# FLATTEN DESIGN - VALIDATE BOOK CHILDREN AND BOOK

import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

from mymodels import Book, BookTag, Highlight, HighlightTag, ReadwiseBatch
from myschemas import BookSchema, BookTagsSchema, HighlightSchema, HighlightTagsSchema
from sqlalchemy.orm import Session

# === Utility Functions ===


def update_if_changed(obj, data: dict) -> bool:
    updated = False
    for field, new_value in data.items():
        if hasattr(obj, field):
            current_value = getattr(obj, field)
            if current_value != new_value:
                setattr(obj, field, new_value)
                updated = True
    return updated


def get_or_create(session: Session, model, pk_field: str, data: dict):
    obj = session.get(model, data[pk_field])
    if obj:
        update_if_changed(obj, data)
    else:
        obj = model(**data)
    session.add(obj)
    return obj


# === Flatten Function ===


def flatten_books_data(
    raw_books: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    books = []
    book_tags = []
    highlights = []
    highlight_tags = []

    for raw_book in raw_books:
        books.append(
            {k: v for k, v in raw_book.items() if k not in ("book_tags", "highlights")}
        )

        for tag in raw_book.get("book_tags", []):
            tag_copy = tag.copy()
            tag_copy["user_book_id"] = raw_book["user_book_id"]
            book_tags.append(tag_copy)

        for hl in raw_book.get("highlights", []):
            hl_copy = {k: v for k, v in hl.items() if k != "tags"}
            hl_copy["user_book_id"] = raw_book["user_book_id"]
            highlights.append(hl_copy)

            for tag in hl.get("tags", []):
                tag_copy = tag.copy()
                tag_copy["highlight_id"] = hl["id"]
                highlight_tags.append(tag_copy)

    return {
        "books": books,
        "book_tags": book_tags,
        "highlights": highlights,
        "highlight_tags": highlight_tags,
    }


# === Validation Functions ===


def validate_objects(
    objs: list[dict[str, Any]], schema_class: Any
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid = []
    invalid = []
    for obj in objs:
        try:
            schema_class(**obj)  # Validate
            valid.append(obj)
        except Exception as err:
            logging.warning(f"Validation failed: {err}")
            invalid.append(obj)
    return valid, invalid


def mark_validated(objs: list[dict[str, Any]], validated: bool) -> list[dict[str, Any]]:
    for obj in objs:
        obj["validated"] = validated
    return objs


def compute_children_valid(
    books: list[dict[str, Any]],
    highlights: list[dict[str, Any]],
    book_tags: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    book_id_to_valid_children = defaultdict(lambda: True)

    for hl in highlights:
        if not hl.get("validated", True):
            book_id_to_valid_children[hl["user_book_id"]] = False

    for tag in book_tags:
        if not tag.get("validated", True):
            book_id_to_valid_children[tag["user_book_id"]] = False

    for book in books:
        book["children_valid"] = book_id_to_valid_children[book["user_book_id"]]

    return books


# === Syncers ===


class BaseSyncer(ABC):
    def __init__(self, data: dict, session: Session, batch: ReadwiseBatch):
        self.data = data
        self.session = session
        self.batch = batch

    @property
    @abstractmethod
    def model_class(self): ...

    @property
    @abstractmethod
    def pk_field(self): ...

    def sync(self):
        obj_data = self.data.copy()
        obj_data["batch"] = self.batch
        get_or_create(self.session, self.model_class, self.pk_field, obj_data)


class BookSyncer(BaseSyncer):
    @property
    def model_class(self):
        return Book

    @property
    def pk_field(self):
        return "user_book_id"


class BookTagSyncer(BaseSyncer):
    @property
    def model_class(self):
        return BookTag

    @property
    def pk_field(self):
        return "id"


class HighlightSyncer(BaseSyncer):
    @property
    def model_class(self):
        return Highlight

    @property
    def pk_field(self):
        return "id"


class HighlightTagSyncer(BaseSyncer):
    @property
    def model_class(self):
        return HighlightTag

    @property
    def pk_field(self):
        return "id"


# === Database Populater ===


class DatabasePopulater:
    def __init__(
        self,
        session: Session,
        flat_data: dict[str, list[dict[str, Any]]],
        start_fetch: datetime,
        end_fetch: datetime,
    ):
        self.session = session
        self.flat_data = flat_data
        self.start_fetch = start_fetch
        self.end_fetch = end_fetch
        self.batch: Optional[ReadwiseBatch] = None

    def populate_database(self):
        self.batch = ReadwiseBatch(
            start_time=self.start_fetch,
            end_time=self.end_fetch,
            database_write_time=datetime.now(),
        )
        self.session.add(self.batch)

        self.populate_books()
        self.populate_book_tags()
        self.populate_highlights()
        self.populate_highlight_tags()

        self.session.commit()

    def populate_books(self):
        for book_data in self.flat_data["books"]:
            BookSyncer(book_data, self.session, self.batch).sync()

    def populate_book_tags(self):
        for tag_data in self.flat_data["book_tags"]:
            BookTagSyncer(tag_data, self.session, self.batch).sync()

    def populate_highlights(self):
        for hl_data in self.flat_data["highlights"]:
            HighlightSyncer(hl_data, self.session, self.batch).sync()

    def populate_highlight_tags(self):
        for tag_data in self.flat_data["highlight_tags"]:
            HighlightTagSyncer(tag_data, self.session, self.batch).sync()


# === Pipeline Orchestration ===


def run_pipeline(
    user_config: Any,
    setup_logging_func,
    get_session_func,
    check_db_func,
    fetch_func,
    flatten_func,
    update_db_func,
) -> None:
    setup_logging_func()
    session = get_session_func(user_config.db_path)
    last_fetch = check_db_func(session, user_config)
    raw_books, start_fetch, end_fetch = fetch_func(last_fetch)

    flat_data = flatten_func(raw_books)

    valid_books, invalid_books = validate_objects(flat_data["books"], BookSchema)
    valid_highlights, invalid_highlights = validate_objects(
        flat_data["highlights"], HighlightSchema
    )
    valid_book_tags, invalid_book_tags = validate_objects(
        flat_data["book_tags"], BookTagsSchema
    )
    valid_highlight_tags, invalid_highlight_tags = validate_objects(
        flat_data["highlight_tags"], HighlightTagsSchema
    )

    all_books = mark_validated(valid_books, True) + mark_validated(invalid_books, False)
    all_highlights = mark_validated(valid_highlights, True) + mark_validated(
        invalid_highlights, False
    )
    all_book_tags = mark_validated(valid_book_tags, True) + mark_validated(
        invalid_book_tags, False
    )
    all_highlight_tags = mark_validated(valid_highlight_tags, True) + mark_validated(
        invalid_highlight_tags, False
    )

    all_books = compute_children_valid(all_books, all_highlights, all_book_tags)

    final_flat_data = {
        "books": all_books,
        "book_tags": all_book_tags,
        "highlights": all_highlights,
        "highlight_tags": all_highlight_tags,
    }

    update_db_func(session, final_flat_data, start_fetch, end_fetch)


def update_database(
    session: Session,
    flat_data: dict[str, list[dict[str, Any]]],
    start_fetch: datetime,
    end_fetch: datetime,
) -> None:
    pop = DatabasePopulater(session, flat_data, start_fetch, end_fetch)
    pop.populate_database()


#  FUNCTIONAL ALTERNATIVE TO SYNCERS


def sync_object(
    session: Session, model_class, pk_field: str, data: dict, batch: ReadwiseBatch
):
    obj_data = data.copy()
    obj_data["batch"] = batch
    get_or_create(session, model_class, pk_field, obj_data)


for book_data in self.flat_data["books"]:
    sync_object(self.session, Book, "user_book_id", book_data, self.batch)


# DESIGN 7

from datetime import datetime
from typing import Any

from mymodels import Book, ReadwiseBatch  # Replace with your actual model imports
from sqlalchemy.orm import Session


def update_if_changed(obj, data: dict) -> bool:
    updated = False
    for field, new_value in data.items():
        if hasattr(obj, field):
            current_value = getattr(obj, field)
            if current_value != new_value:
                setattr(obj, field, new_value)
                updated = True
    return updated


def get_or_create(session: Session, model, pk_field: str, data: dict):
    obj = session.get(model, data[pk_field])
    if obj:
        update_if_changed(obj, data)
    else:
        obj = model(**data)
    return obj


def deschema_books(validated_books: list[BookSchema]) -> list[dict[str, Any]]:
    """
    Convert a list of validated BookSchema objects back into dicts.

    Parameters
    ----------
    validated_books : list[BookSchema]
        List of Pydantic validated books.

    Returns
    -------
    list[dict]
        List of plain dictionaries.
    """
    return [book.model_dump() for book in validated_books]


class BookSyncer:
    def __init__(
        self,
        book_data: dict[str, Any],
        session: Session,
        batch: ReadwiseBatch,
        validated: bool,
    ):
        self.book_data = book_data
        self.session = session
        self.batch = batch
        self.validated = validated

    def sync(self) -> Book:
        core_data = {
            k: v
            for k, v in self.book_data.items()
            if k not in {"book_tags", "highlights"}
        }
        core_data["batch"] = self.batch
        core_data["validated"] = self.validated
        book = get_or_create(self.session, Book, "user_book_id", core_data)
        return book


class BookTagSyncer:
    def __init__(
        self,
        tag_dicts: list[dict[str, Any]],
        user_book_id: int,
        session: Session,
        batch: ReadwiseBatch,
        validated: bool,
    ):
        self.tag_dicts = tag_dicts
        self.user_book_id = user_book_id
        self.session = session
        self.batch = batch
        self.validated = validated

    def sync(self) -> list[BookTag]:
        tags = []
        for tag_data in self.tag_dicts:
            tag_data = tag_data.copy()
            tag_data["user_book_id"] = self.user_book_id
            tag_data["batch"] = self.batch
            tag_data["validated"] = self.validated
            tag = get_or_create(self.session, BookTag, "name", tag_data)
            tags.append(tag)
        return tags


class HighlightSyncer:
    def __init__(
        self,
        hl_data: dict[str, Any],
        user_book_id: int,
        session: Session,
        batch: ReadwiseBatch,
        validated: bool,
    ):
        self.hl_data = hl_data
        self.user_book_id = user_book_id
        self.session = session
        self.batch = batch
        self.validated = validated

    def sync(self) -> Highlight:
        hl_data = {k: v for k, v in self.hl_data.items() if k != "tags"}
        hl_data["user_book_id"] = self.user_book_id
        hl_data["batch"] = self.batch
        hl_data["validated"] = self.validated
        highlight = get_or_create(self.session, Highlight, "highlight_id", hl_data)
        return highlight


class HighlightTagSyncer:
    def __init__(
        self,
        tag_dicts: list[dict[str, Any]],
        highlight_id: int,
        session: Session,
        batch: ReadwiseBatch,
        validated: bool,
    ):
        self.tag_dicts = tag_dicts
        self.highlight_id = highlight_id
        self.session = session
        self.batch = batch
        self.validated = validated

    def sync(self) -> list[HighlightTag]:
        tags = []
        for tag_data in self.tag_dicts:
            tag_data = tag_data.copy()
            tag_data["highlight_id"] = self.highlight_id
            tag_data["batch"] = self.batch
            tag_data["validated"] = self.validated
            tag = get_or_create(self.session, HighlightTag, "name", tag_data)
            tags.append(tag)
        return tags


class DatabasePopulater:
    def __init__(
        self,
        session: Session,
        books_data: list[dict[str, Any]],
        start_fetch: datetime,
        end_fetch: datetime,
        validated: bool,
    ):
        self.session = session
        self.books_data = books_data
        self.start_fetch = start_fetch
        self.end_fetch = end_fetch
        self.validated = validated
        self.batch: Optional[ReadwiseBatch] = None

    def populate_database(self) -> None:
        self.batch = ReadwiseBatch(
            start_time=self.start_fetch,
            end_time=self.end_fetch,
            database_write_time=datetime.now(),
        )
        self.session.add(self.batch)

        for book_data in self.books_data:
            self.populate_single_book(book_data)

        self.session.commit()

    def populate_single_book(self, book_data: dict[str, Any]) -> None:
        book = BookSyncer(book_data, self.session, self.batch, self.validated).sync()

        book.book_tags = BookTagSyncer(
            book_data.get("book_tags", []),
            book.user_book_id,
            self.session,
            self.batch,
            self.validated,
        ).sync()

        highlights = []
        for hl_data in book_data["highlights"]:
            highlight = HighlightSyncer(
                hl_data, book.user_book_id, self.session, self.batch, self.validated
            ).sync()
            highlight.highlight_tags = HighlightTagSyncer(
                hl_data.get("tags", []),
                highlight.id,
                self.session,
                self.batch,
                self.validated,
            ).sync()
            highlights.append(highlight)

        book.highlights = highlights
        self.session.add(book)


# DESIGN 6 - use Pydantic schemas (last min model dump)

from datetime import datetime

from mymodels import (  # Replace with your actual model imports
    Book,
    BookTag,
    Highlight,
    HighlightTag,
    ReadwiseBatch,
)
from myschemas import BookSchema  # Replace with your actual Pydantic schema imports
from sqlalchemy.orm import Session


def update_if_changed(obj, data: dict) -> bool:
    updated = False
    for field, new_value in data.items():
        if hasattr(obj, field):
            current_value = getattr(obj, field)
            if current_value != new_value:
                setattr(obj, field, new_value)
                updated = True
    return updated


def get_or_create(session: Session, model, pk_field: str, data: dict):
    obj = session.get(model, data[pk_field])
    if obj:
        update_if_changed(obj, data)
    else:
        obj = model(**data)
    return obj


class BookSyncer:
    def __init__(self, book_schema: BookSchema, session: Session, batch: ReadwiseBatch):
        self.book_schema = book_schema
        self.session = session
        self.batch = batch

    def sync(self) -> Book:
        core_data = self.book_schema.model_dump(exclude={"book_tags", "highlights"})
        core_data["batch"] = self.batch
        book = get_or_create(self.session, Book, "user_book_id", core_data)
        return book


class BookTagSyncer:
    def __init__(
        self,
        tag_schemas: list[BookTagsSchema],
        user_book_id: int,
        session: Session,
        batch: ReadwiseBatch,
    ):
        self.tag_schemas = tag_schemas
        self.user_book_id = user_book_id
        self.session = session
        self.batch = batch

    def sync(self) -> list[BookTag]:
        tags = []
        for tag_schema in self.tag_schemas:
            tag_data = tag_schema.model_dump()
            tag_data["user_book_id"] = self.user_book_id
            tag_data["batch"] = self.batch
            tag = get_or_create(self.session, BookTag, "name", tag_data)
            tags.append(tag)
        return tags


class HighlightSyncer:
    def __init__(
        self,
        hl_schema: HighlightSchema,
        user_book_id: int,
        session: Session,
        batch: ReadwiseBatch,
    ):
        self.hl_schema = hl_schema
        self.user_book_id = user_book_id
        self.session = session
        self.batch = batch

    def sync(self) -> Highlight:
        hl_data = self.hl_schema.model_dump(exclude={"tags"})
        hl_data["user_book_id"] = self.user_book_id
        hl_data["batch"] = self.batch
        highlight = get_or_create(self.session, Highlight, "highlight_id", hl_data)
        return highlight


class HighlightTagSyncer:
    def __init__(
        self,
        tag_schemas: list[HighlightTagsSchema],
        highlight_id: int,
        session: Session,
        batch: ReadwiseBatch,
    ):
        self.tag_schemas = tag_schemas
        self.highlight_id = highlight_id
        self.session = session
        self.batch = batch

    def sync(self) -> list[HighlightTag]:
        tags = []
        for tag_schema in self.tag_schemas:
            tag_data = tag_schema.model_dump()
            tag_data["highlight_id"] = self.highlight_id
            tag_data["batch"] = self.batch
            tag = get_or_create(self.session, HighlightTag, "name", tag_data)
            tags.append(tag)
        return tags


class DatabasePopulater:
    def __init__(
        self,
        session: Session,
        books_data: list[BookSchema],
        start_fetch: datetime,
        end_fetch: datetime,
    ):
        self.session = session
        self.books_data = books_data
        self.start_fetch = start_fetch
        self.end_fetch = end_fetch
        self.batch: Optional[ReadwiseBatch] = None

    def populate_database(self) -> None:
        self.batch = ReadwiseBatch(
            start_time=self.start_fetch,
            end_time=self.end_fetch,
            database_write_time=datetime.now(),
        )
        self.session.add(self.batch)

        for book_schema in self.books_data:
            self.populate_single_book(book_schema)

        self.session.commit()

    def populate_single_book(self, book_schema: BookSchema) -> None:
        book = BookSyncer(book_schema, self.session, self.batch).sync()

        book.book_tags = BookTagSyncer(
            book_schema.book_tags, book.user_book_id, self.session, self.batch
        ).sync()

        highlights = []
        for hl_schema in book_schema.highlights:
            highlight = HighlightSyncer(
                hl_schema, book.user_book_id, self.session, self.batch
            ).sync()
            highlight.highlight_tags = HighlightTagSyncer(
                hl_schema.tags, highlight.id, self.session, self.batch
            ).sync()
            highlights.append(highlight)

        book.highlights = highlights
        self.session.add(book)


# DESIGN 5 - add Readwise batches

from datetime import datetime

from mymodels import (  # Replace with your actual model imports
    Book,
    BookTag,
    Highlight,
    HighlightTag,
    ReadwiseBatch,
)
from sqlalchemy.orm import Session


def update_if_changed(obj, data: dict) -> bool:
    updated = False
    for field, new_value in data.items():
        if hasattr(obj, field):
            current_value = getattr(obj, field)
            if current_value != new_value:
                setattr(obj, field, new_value)
                updated = True
    return updated


def get_or_create(session: Session, model, pk_field: str, data: dict):
    obj = session.get(model, data[pk_field])
    if obj:
        update_if_changed(obj, data)
    else:
        obj = model(**data)
    return obj


class BookSyncer:
    def __init__(self, book_data: dict, session: Session, batch: ReadwiseBatch):
        self.book_data = book_data
        self.session = session
        self.batch = batch

    def sync(self) -> Book:
        core_data = {
            k: v
            for k, v in self.book_data.items()
            if k not in {"book_tags", "highlights"}
        }
        core_data["batch"] = self.batch
        book = get_or_create(self.session, Book, "user_book_id", core_data)
        return book


class BookTagSyncer:
    def __init__(
        self,
        tag_dicts: list[dict],
        user_book_id: str,
        session: Session,
        batch: ReadwiseBatch,
    ):
        self.tag_dicts = tag_dicts
        self.user_book_id = user_book_id
        self.session = session
        self.batch = batch

    def sync(self) -> list[BookTag]:
        tags = []
        for tag_data in self.tag_dicts:
            tag_data["user_book_id"] = self.user_book_id
            tag_data["batch"] = self.batch
            tag = get_or_create(self.session, BookTag, "name", tag_data)
            tags.append(tag)
        return tags


class HighlightSyncer:
    def __init__(
        self, hl_data: dict, user_book_id: str, session: Session, batch: ReadwiseBatch
    ):
        self.hl_data = hl_data
        self.user_book_id = user_book_id
        self.session = session
        self.batch = batch

    def sync(self) -> Highlight:
        self.hl_data["user_book_id"] = self.user_book_id
        self.hl_data["batch"] = self.batch
        highlight = get_or_create(self.session, Highlight, "highlight_id", self.hl_data)
        return highlight


class HighlightTagSyncer:
    def __init__(
        self,
        tag_dicts: list[dict],
        highlight_id: str,
        session: Session,
        batch: ReadwiseBatch,
    ):
        self.tag_dicts = tag_dicts
        self.highlight_id = highlight_id
        self.session = session
        self.batch = batch

    def sync(self) -> list[HighlightTag]:
        tags = []
        for tag_data in self.tag_dicts:
            tag_data["highlight_id"] = self.highlight_id
            tag_data["batch"] = self.batch
            tag = get_or_create(self.session, HighlightTag, "name", tag_data)
            tags.append(tag)
        return tags


class DatabasePopulater:
    def __init__(
        self,
        session: Session,
        books_data: list[dict],
        start_fetch: datetime,
        end_fetch: datetime,
    ):
        self.session = session
        self.books_data = books_data
        self.start_fetch = start_fetch
        self.end_fetch = end_fetch
        self.batch: Optional[ReadwiseBatch] = None

    def populate_database(self) -> None:
        self.batch = ReadwiseBatch(
            start_time=self.start_fetch,
            end_time=self.end_fetch,
            database_write_time=datetime.now(),
        )
        self.session.add(self.batch)

        for book_data in self.books_data:
            self.populate_single_book(book_data)

        self.session.commit()

    def populate_single_book(self, book_data: dict) -> None:
        book = BookSyncer(book_data, self.session, self.batch).sync()

        book.book_tags = BookTagSyncer(
            book_data.get("book_tags", []), book.user_book_id, self.session, self.batch
        ).sync()

        highlights = []
        for hl_data in book_data["highlights"]:
            highlight = HighlightSyncer(
                hl_data, book.user_book_id, self.session, self.batch
            ).sync()
            highlight.highlight_tags = HighlightTagSyncer(
                hl_data.get("highlight_tags", []),
                highlight.highlight_id,
                self.session,
                self.batch,
            ).sync()
            highlights.append(highlight)

        book.highlights = highlights
        self.session.add(book)


# DESIGN 4 - SPLIT OUT BOOK ADDER

from mymodels import (  # Replace with your actual model imports
    Book,
    BookTag,
    Highlight,
    HighlightTag,
)
from sqlalchemy.orm import Session


def update_if_changed(obj, data: dict) -> bool:
    updated = False
    for field, new_value in data.items():
        if hasattr(obj, field):
            current_value = getattr(obj, field)
            if current_value != new_value:
                setattr(obj, field, new_value)
                updated = True
    return updated


def get_or_create(session: Session, model, pk_field: str, data: dict):
    obj = session.get(model, data[pk_field])
    if obj:
        update_if_changed(obj, data)
    else:
        obj = model(**data)
    return obj


class BookSyncer:
    def __init__(self, book_data: dict, session: Session):
        self.book_data = book_data
        self.session = session

    def sync(self) -> Book:
        core_data = {
            k: v
            for k, v in self.book_data.items()
            if k not in {"book_tags", "highlights"}
        }
        book = get_or_create(self.session, Book, "user_book_id", core_data)
        return book


class BookTagSyncer:
    def __init__(self, tag_dicts: list[dict], user_book_id: str, session: Session):
        self.tag_dicts = tag_dicts
        self.user_book_id = user_book_id
        self.session = session

    def sync(self) -> list[BookTag]:
        tags = []
        for tag_data in self.tag_dicts:
            tag_data["user_book_id"] = self.user_book_id
            tag = get_or_create(self.session, BookTag, "name", tag_data)
            tags.append(tag)
        return tags


class HighlightSyncer:
    def __init__(self, hl_data: dict, user_book_id: str, session: Session):
        self.hl_data = hl_data
        self.user_book_id = user_book_id
        self.session = session

    def sync(self) -> Highlight:
        self.hl_data["user_book_id"] = self.user_book_id
        highlight = get_or_create(self.session, Highlight, "highlight_id", self.hl_data)
        return highlight


class HighlightTagSyncer:
    def __init__(self, tag_dicts: list[dict], highlight_id: str, session: Session):
        self.tag_dicts = tag_dicts
        self.highlight_id = highlight_id
        self.session = session

    def sync(self) -> list[HighlightTag]:
        tags = []
        for tag_data in self.tag_dicts:
            tag_data["highlight_id"] = self.highlight_id
            tag = get_or_create(self.session, HighlightTag, "name", tag_data)
            tags.append(tag)
        return tags


class DatabasePopulater:
    def __init__(self, session: Session, books_data: list[dict]):
        self.session = session
        self.books_data = books_data

    def populate_database(self) -> None:
        for book_data in self.books_data:
            self.populate_single_book(book_data)
        self.session.commit()

    def populate_single_book(self, book_data: dict) -> None:
        book = BookSyncer(book_data, self.session).sync()

        book.book_tags = BookTagSyncer(
            book_data.get("book_tags", []), book.user_book_id, self.session
        ).sync()

        highlights = []
        for hl_data in book_data["highlights"]:
            highlight = HighlightSyncer(hl_data, book.user_book_id, self.session).sync()
            highlight.highlight_tags = HighlightTagSyncer(
                hl_data.get("highlight_tags", []), highlight.highlight_id, self.session
            ).sync()
            highlights.append(highlight)

        book.highlights = highlights
        self.session.add(book)


# DESIGN THREE

from mymodels import (  # Replace with your actual model imports
    Book,
    BookTag,
    Highlight,
    HighlightTag,
)
from sqlalchemy.orm import Session


def update_if_changed(obj, data: dict) -> bool:
    updated = False
    for field, new_value in data.items():
        if hasattr(obj, field):
            current_value = getattr(obj, field)
            if current_value != new_value:
                setattr(obj, field, new_value)
                updated = True
    return updated


def get_or_create(session: Session, model, pk_field: str, data: dict):
    obj = session.get(model, data[pk_field])
    if obj:
        update_if_changed(obj, data)
    else:
        obj = model(**data)
    return obj


class BookSyncer:
    def __init__(self, book_data: dict, session: Session):
        self.book_data = book_data
        self.session = session

    def sync(self) -> Book:
        core_data = {
            k: v
            for k, v in self.book_data.items()
            if k not in {"book_tags", "highlights"}
        }
        book = get_or_create(self.session, Book, "user_book_id", core_data)
        return book


class BookTagSyncer:
    def __init__(self, tag_dicts: list[dict], user_book_id: str, session: Session):
        self.tag_dicts = tag_dicts
        self.user_book_id = user_book_id
        self.session = session

    def sync(self) -> list[BookTag]:
        tags = []
        for tag_data in self.tag_dicts:
            tag_data["user_book_id"] = self.user_book_id
            tag = get_or_create(self.session, BookTag, "name", tag_data)
            tags.append(tag)
        return tags


class HighlightSyncer:
    def __init__(self, hl_data: dict, user_book_id: str, session: Session):
        self.hl_data = hl_data
        self.user_book_id = user_book_id
        self.session = session

    def sync(self) -> Highlight:
        self.hl_data["user_book_id"] = self.user_book_id
        highlight = get_or_create(self.session, Highlight, "highlight_id", self.hl_data)
        return highlight


class HighlightTagSyncer:
    def __init__(self, tag_dicts: list[dict], highlight_id: str, session: Session):
        self.tag_dicts = tag_dicts
        self.highlight_id = highlight_id
        self.session = session

    def sync(self) -> list[HighlightTag]:
        tags = []
        for tag_data in self.tag_dicts:
            tag_data["highlight_id"] = self.highlight_id
            tag = get_or_create(self.session, HighlightTag, "name", tag_data)
            tags.append(tag)
        return tags


class DatabasePopulater:
    def __init__(self, session: Session, books_data: list[dict]):
        self.session = session
        self.books_data = books_data

    def populate_database(self) -> None:
        for book_data in self.books_data:
            book = BookSyncer(book_data, self.session).sync()

            book.book_tags = BookTagSyncer(
                book_data.get("book_tags", []), book.user_book_id, self.session
            ).sync()

            highlights = []
            for hl_data in book_data["highlights"]:
                highlight = HighlightSyncer(
                    hl_data, book.user_book_id, self.session
                ).sync()
                highlight.highlight_tags = HighlightTagSyncer(
                    hl_data.get("highlight_tags", []),
                    highlight.highlight_id,
                    self.session,
                ).sync()
                highlights.append(highlight)

            book.highlights = highlights

            self.session.add(book)

        self.session.commit()


# DESIGN TWO

from mymodels import (  # Replace with your actual model imports
    Book,
    BookTag,
    Highlight,
    HighlightTag,
)
from sqlalchemy.orm import Session


def update_if_changed(obj, data: dict) -> bool:
    updated = False
    for field, new_value in data.items():
        if hasattr(obj, field):
            current_value = getattr(obj, field)
            if current_value != new_value:
                setattr(obj, field, new_value)
                updated = True
    return updated


def get_or_create(session: Session, model, pk_field: str, data: dict):
    obj = session.get(model, data[pk_field])
    if obj:
        update_if_changed(obj, data)
    else:
        obj = model(**data)
    return obj


class BookSyncer:
    def __init__(self, book_data: dict, session: Session):
        self.book_data = book_data
        self.session = session

    def sync(self) -> Book:
        core_data = {
            k: v
            for k, v in self.book_data.items()
            if k not in {"book_tags", "highlights"}
        }
        book = get_or_create(self.session, Book, "user_book_id", core_data)
        self.session.add(book)
        return book


class BookTagSyncer:
    def __init__(self, tag_dicts: list[dict], user_book_id: str, session: Session):
        self.tag_dicts = tag_dicts
        self.user_book_id = user_book_id
        self.session = session

    def sync(self) -> list[BookTag]:
        tags = []
        for tag_data in self.tag_dicts:
            tag_data["user_book_id"] = self.user_book_id
            tag = get_or_create(self.session, BookTag, "name", tag_data)
            tags.append(tag)
        return tags


class HighlightSyncer:
    def __init__(self, hl_data: dict, user_book_id: str, session: Session):
        self.hl_data = hl_data
        self.user_book_id = user_book_id
        self.session = session

    def sync(self) -> Highlight:
        self.hl_data["user_book_id"] = self.user_book_id
        highlight = get_or_create(self.session, Highlight, "highlight_id", self.hl_data)
        self.session.add(highlight)
        return highlight


class HighlightTagSyncer:
    def __init__(self, tag_dicts: list[dict], highlight_id: str, session: Session):
        self.tag_dicts = tag_dicts
        self.highlight_id = highlight_id
        self.session = session

    def sync(self) -> list[HighlightTag]:
        tags = []
        for tag_data in self.tag_dicts:
            tag_data["highlight_id"] = self.highlight_id
            tag = get_or_create(self.session, HighlightTag, "name", tag_data)
            tags.append(tag)
        return tags


class DatabasePopulater:
    def __init__(self, session: Session, books_data: list[dict]):
        self.session = session
        self.books_data = books_data

    def populate_database(self) -> None:
        for book_data in self.books_data:
            book = BookSyncer(book_data, self.session).sync()
            BookTagSyncer(
                book_data.get("book_tags", []), book.user_book_id, self.session
            ).sync()

            for hl_data in book_data["highlights"]:
                highlight = HighlightSyncer(
                    hl_data, book.user_book_id, self.session
                ).sync()
                HighlightTagSyncer(
                    hl_data.get("highlight_tags", []),
                    highlight.highlight_id,
                    self.session,
                ).sync()

        self.session.commit()


# DESIGN ONE - nested

from sqlalchemy.orm import Session

# from mymodels import Book, Highlight, BookTag, HighlightTag  # Replace with your actual model imports


def update_if_changed(obj, data: dict) -> bool:
    updated = False
    for field, new_value in data.items():
        if hasattr(obj, field):
            current_value = getattr(obj, field)
            if current_value != new_value:
                setattr(obj, field, new_value)
                updated = True
    return updated


def get_or_create(session: Session, model, pk_field: str, data: dict):
    obj = session.get(model, data[pk_field])
    if obj:
        update_if_changed(obj, data)
    else:
        obj = model(**data)
    return obj


class HighlightSyncer:
    def __init__(self, session: Session, highlight_data: dict, parent_book: Book):
        self.session = session
        self.data = highlight_data
        self.book = parent_book

    def sync(self) -> Highlight:
        tag_dicts = self.data.pop("highlight_tags", [])
        self.data["user_book_id"] = self.book.user_book_id

        highlight = get_or_create(self.session, Highlight, "highlight_id", self.data)
        highlight.highlight_tags = [self._sync_tag(tag_data) for tag_data in tag_dicts]
        return highlight

    def _sync_tag(self, tag_data: dict) -> HighlightTag:
        tag_data["highlight_id"] = self.data["highlight_id"]
        return get_or_create(self.session, HighlightTag, "name", tag_data)


class BookSyncer:
    def __init__(self, book_data: dict, session: Session):
        self.session = session
        self.book_data = book_data
        self.book: Optional[Book] = None

    def sync(self) -> Book:
        self.book = self._sync_core_book()
        self.book.book_tags = self._sync_book_tags()
        self.book.highlights = self._sync_highlights()
        self.session.add(self.book)
        return self.book

    def _sync_core_book(self) -> Book:
        core_data = {
            k: v
            for k, v in self.book_data.items()
            if k not in {"book_tags", "highlights"}
        }
        return get_or_create(self.session, Book, "user_book_id", core_data)

    def _sync_book_tags(self) -> list[BookTag]:
        tag_dicts = self.book_data.get("book_tags", [])
        return [self._sync_book_tag(tag_data) for tag_data in tag_dicts]

    def _sync_book_tag(self, tag_data: dict) -> BookTag:
        tag_data["user_book_id"] = self.book.user_book_id
        return get_or_create(self.session, BookTag, "name", tag_data)

    def _sync_highlights(self) -> list[Highlight]:
        highlight_dicts = self.book_data["highlights"]
        return [
            HighlightSyncer(self.session, hl_data, self.book).sync()
            for hl_data in highlight_dicts
        ]


class DatabasePopulater:
    def __init__(self, session: Session, books_data: list[dict]):
        self.session = session
        self.books_data = books_data

    def populate_database(self) -> None:
        for book_data in self.books_data:
            book_syncer = BookSyncer(book_data, self.session)
            book_syncer.sync()

        self.session.commit()
