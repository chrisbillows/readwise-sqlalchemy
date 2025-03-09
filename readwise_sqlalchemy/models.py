"""
SQLAlchemy ORM models, associated types and validators.

The models Book, Highlight, HighlightTag and ReadwiseBatch are nested and intended to
be used in unison.

Readwise primary keys are used as database primary keys throughout for consistency with
Readwise object relationships.

Readwise API responses assume and rely upon Pydantic validation.

Note on ORM validation
----------------------
Mapped classes may seem to validate things that they actually don't:

- Type hints e.g. ``Mapped[int]`` and character limits e.g. ``String(511)`` are not
  enforced at runtime by SQLAlchemy. The underlying database dialect may - or may not -
  enforce them. SQLite is particularly permissive and enforces neither datatype nor
  character limits.

- Missing fields are accepted and default to None. This only results in an error when
  committing to the database and only if the field is ``nullable=False``.

"""

from datetime import datetime
from typing import Optional

from sqlalchemy import Dialect, ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import TypeDecorator


class CommaSeparatedList(TypeDecorator):
    """
    Convert to/from a list of strings/a comma separated list as a single string.

    A custom SQLAlchemy type extending the SQL String type via TypeDecorator to store a
    list of strings as a comma separated list, rendered as a single str. (SQLite has no
    list types). The custom behaviour is provided by overriding the TypeDecorator
    methods ``process_bind_param`` and  ``process_result_value``.

                list[str]                                   str^
                    |                                        |
                    |                                        |
                write to db                             read from db
                    |                                        |
                    |                                        |
                    v                                        V
                   str^                                   list[str]

    ^The str is a comma separated list rendered as a single string.

    See:
    https://docs.sqlalchemy.org/en/20/core/custom_types.html#augmenting-existing-types
    """

    impl = String
    # Safe to cache as the same values will always give the same results.
    cache_ok = True

    def process_bind_param(self, value: list[str], dialect: Dialect) -> str:
        """
        Receive a bound parameter of type ``list[str]`` and convert to ``str``.

        Override the TypeDecorator method. The method is called at statement execution
        time and is passed the literal Python data value which is to be associated with
        a bound parameter in the statement.

        Parameters
        ----------
        value : list[str]
            Data to operate upon, here a list of strings.

        dialect : Dialect
            The SQL Dialect in use.

        Returns
        -------
        str
            A single Python string representing a list of strings as comma separated
            values.
        """
        return "" if not value else ",".join(value)

    def process_result_value(self, value: str, dialect: Dialect) -> list[str]:
        """
        Receive a result-row column value of type ``str` and convert to ``list[str]``.

        This method is called at result fetching time and is passed the literal Python
        data value extracted from a database result row.

        Parameters
        ----------
        value: str
            Data to operate upon, here a string representing a list of strings as comma
            separated values.

        dialect: Dialect
            The SQL Dialect in use.

        Returns
        -------
        list[str]
            A list of strings.
        """
        return [] if value == "" else value.split(",")


class Base(DeclarativeBase):
    """
    Subclass SQLAlchemy ``DeclarativeBase`` base class.

    All ORM Mapped classes should inherit from ``Base``. Tables can then be created
    with ``Base.metadata.create_all``.
    """

    pass


class Book(Base):
    """
    Readwise book as a SQL Alchemy ORM Mapped class.

    Validation is enforced in the Pydantic layer only. For example, the class will
    accept null values for all fields, aside from the primary key - even those fields
    which should never be null. Using API data directly with this class may result in
    unexpected behaviour and it not recommended.

    Each instance corresponds to a book dictionary from the Readwise 'Highlight EXPORT'
    endpoint. "books" are the parent objects for all highlights, even those not sourced
    from books. Examples:

    +------------+--------------------------------------------------------------------+
    | Source     | Parent Object                                                      |
    +============+====================================================================+
    | book       | Book                                                               |
    +------------+--------------------------------------------------------------------+
    | x.post     | "Tweets from @<user>". Saved posts are highlights in that entry.   |
    +------------+--------------------------------------------------------------------+
    | x.thread   | Threads are parent objects. Titles are text from the first post.   |
    +------------+--------------------------------------------------------------------+
    | podcast    | The podcast episode.                                               |
    +------------+--------------------------------------------------------------------+
    | article    | The article.                                                       |
    +------------+--------------------------------------------------------------------+
    | youtube    | Individual videos are treated as an article.                       |
    +------------+--------------------------------------------------------------------+

    Attributes
    ----------
    user_book_id : int
        Primary key. Unique identifier sourced from Readwise.
    title: str
        The book's title.
    author: str
        The book's author.
    readable_title :

    source : str

    cover_image_url : str

    unique_url : str

    summary : str

    book_tags : list[str]

    category : str

    document_note : str

    readwise_url : str

    source_url : str

    asin : str

    batch_id:
        Foreign key linking the `id` of the associated `ReadwiseBatch`.

    highlights: list[Highlight]
        A list of highlights sourced from the book.
    batch: ReadwiseBatch
        The batch object the book was imported in.
    """

    __tablename__ = "books"

    user_book_id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[Optional[str]] = mapped_column()
    author: Mapped[Optional[str]] = mapped_column()
    readable_title: Mapped[Optional[str]] = mapped_column()
    source: Mapped[Optional[str]] = mapped_column()
    cover_image_url: Mapped[Optional[str]] = mapped_column()
    unique_url: Mapped[Optional[str]] = mapped_column()
    summary: Mapped[Optional[str]] = mapped_column()
    book_tags: Mapped[Optional[str]] = mapped_column(CommaSeparatedList)
    category: Mapped[Optional[str]] = mapped_column()
    document_note: Mapped[Optional[str]] = mapped_column()
    readwise_url: Mapped[Optional[str]] = mapped_column()
    source_url: Mapped[Optional[str]] = mapped_column()
    asin: Mapped[Optional[str]] = mapped_column()

    batch_id: Mapped[int] = mapped_column(ForeignKey("readwise_batches.id"))

    highlights: Mapped[list["Highlight"]] = relationship(back_populates="book")
    batch: Mapped["ReadwiseBatch"] = relationship(back_populates="books")

    def __repr__(self) -> str:
        return (
            f"Book(user_book_id={self.user_book_id!r}, title={self.title!r}, "
            f"highlights={len(self.highlights)})"
        )


class Highlight(Base):
    """
    Readwise highlight as a SQL Alchemy ORM Mapped class.

    Each instance corresponds to a highlight dictionary from the Readwise 'Highlight
    EXPORT' endpoint. Highlights are text excerpts saved by the user from books,
    articles, or other sources.

    Validation is enforced in the Pydantic layer only. For example, the class will
    accept null values for all fields, aside from the primary key - even those fields
    which should never be null. Using API data directly with this class may result in
    unexpected behaviour and is not recommended.

    Attributes
    ----------
    id : int
        Primary key. Unique identifier sourced from Readwise.
    text: str
        The actual highlighted text content.

    user_book_id : int
        Foreign key linking the `user_book_id` of the associated `Book`.
    batch_id : int
        Foreign key linking the `id` of the associated `ReadwiseBatch`.

    book : Book
        The book that this highlight belongs to.
    tags : list[HighlightTag]
        Tags assigned to this highlight.
    batch : ReadwiseBatch
        The batch object the highlight was imported in.
    """

    __tablename__ = "highlights"

    id: Mapped[int] = mapped_column(primary_key=True)
    text: Mapped[str] = mapped_column(String(8191))
    location: Mapped[Optional[int]] = mapped_column()
    location_type: Mapped[Optional[str]] = mapped_column()
    note: Mapped[Optional[str]] = mapped_column()
    color: Mapped[Optional[str]] = mapped_column()
    highlighted_at: Mapped[Optional[datetime]] = mapped_column()
    created_at: Mapped[Optional[datetime]] = mapped_column()
    updated_at: Mapped[Optional[datetime]] = mapped_column()
    external_id: Mapped[Optional[str]] = mapped_column()
    end_location: Mapped[Optional[int]] = mapped_column()
    url: Mapped[Optional[str]] = mapped_column()
    is_favorite: Mapped[Optional[bool]] = mapped_column()
    is_discard: Mapped[Optional[bool]] = mapped_column()
    readwise_url: Mapped[Optional[str]] = mapped_column()

    book_id: Mapped[int] = mapped_column(
        ForeignKey("books.user_book_id"), nullable=False
    )
    batch_id: Mapped[int] = mapped_column(
        ForeignKey("readwise_batches.id"), nullable=False
    )

    book: Mapped["Book"] = relationship(back_populates="highlights")
    tags: Mapped[list["HighlightTag"]] = relationship(back_populates="highlight")
    batch: Mapped["ReadwiseBatch"] = relationship(back_populates="highlights")

    def __repr__(self) -> str:
        parts = [f"Highlight(id={self.id!r}"]
        if self.book:
            parts.append(f"book={self.book.title!r}")
        if self.text:
            truncated_highlight_txt = (
                self.text[:30] + "..." if len(self.text) > 30 else self.text
            )
            parts.append(f"text={truncated_highlight_txt!r}")
        else:
            parts.append(f"text={self.text!r}")
        return ", ".join(parts) + ")"


class HighlightTag(Base):
    """
    Readwise highlight tag as a SQL Alchemy ORM Mapped class.

    Each instance corresponds to a highlight tags dictionary from the Readwise
    'Highlight EXPORT' endpoint.

    Validation is enforced in the Pydantic layer only. For example, the class will
    accept null values for all fields, aside from the primary key - even those fields
    which should never be null. Using API data directly with this class may result in
    unexpected behaviour and is not recommended.

    Attributes
    ----------
    id : int
        Primary key. Unique identifier sourced from Readwise.
    name : str
        The name of the tag. Each tag has an id and name. ``name``s are often common
        across tags/highlights. ``id`` is always unique. E.g. Many highlights may be
        tagged ``favourite`` but each ``favourite`` tag  will be associated with its own
        unique ``id``. Group by ``name`` for this attribute.

    highlight_id : int
        Foreign key linking the ``id`` of the associated ``Highlight``.
    batch_id : int
        Foreign key linking the `id` of the associated `ReadwiseBatch`.

    highlight: Highlight
        The highlight object the tag is associated with.
    batch: ReadwiseBatch
        The batch object the tag was imported in.
    """

    __tablename__ = "highlight_tags"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]

    highlight_id: Mapped[int] = mapped_column(
        ForeignKey("highlights.id"), nullable=False
    )
    batch_id: Mapped[int] = mapped_column(
        ForeignKey("readwise_batches.id"), nullable=False
    )

    highlight: Mapped["Highlight"] = relationship(back_populates="tags")
    batch: Mapped["ReadwiseBatch"] = relationship(back_populates="highlight_tags")

    def __repr__(self) -> str:
        return f"HighlightTag(name={self.name!r}, id={self.id!r})"


class ReadwiseBatch(Base):
    """
    A batch of database updates from the Readwise API.

    This is not API data, therefore validation is performed here.

    Attributes
    ----------
    id : int
        Primary key. Auto generated unique identifier for the batch .
    start_time : datetime
        The start time of a fetch from the API.
    end_time : datetime
        The time the fetch completed.
    database_write_time : Optional[datetime]
        The time the batch was written to the database. Can be None if unset but this is
        intended only to allow this attribute to be added last.

    books : list[Book]
        The books included in the batch.
    highlights : list[Highlight]
        The highlights included in the batch.
    highlight_tags : list[HighlightTag]
        The highlight tags included in the batch.
    """

    __tablename__ = "readwise_batches"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    start_time: Mapped[datetime] = mapped_column(nullable=False)
    end_time: Mapped[datetime] = mapped_column(nullable=False)
    database_write_time: Mapped[datetime] = mapped_column(nullable=True)

    books: Mapped[list["Book"]] = relationship(back_populates="batch")
    highlights: Mapped[list["Highlight"]] = relationship(back_populates="batch")
    highlight_tags: Mapped[list["HighlightTag"]] = relationship(back_populates="batch")

    def __repr__(self) -> str:
        parts = [f"ReadwiseBatch(id={self.id!r}"]
        parts.append(f"books={len(self.books)}")
        parts.append(f"highlights={len(self.highlights)}")
        parts.append(f"highlight_tags={len(self.highlight_tags)}")
        if self.start_time:
            parts.append(f"start={self.start_time.isoformat()}")
        if self.end_time:
            parts.append(f"end={self.end_time.isoformat()}")
        if self.database_write_time:
            parts.append(f"write={self.database_write_time.isoformat()}")
        return ", ".join(parts) + ")"


# def convert_iso_to_datetime(date_str: Any | None) -> datetime | None:
#     """Convert an ISO 8601 string to a datetime object."""
#     return datetime.fromisoformat(date_str.replace("Z", "+00:00")) if date_str else None

# class Base(DeclarativeBase):
#     __allow_unmapped__ = True
