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
from typing import Any, Optional, cast

from sqlalchemy import Dialect, ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import TypeDecorator


class CommaSeparatedList(TypeDecorator[list[str]]):
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

    def process_bind_param(self, value: Any, dialect: Dialect) -> str:
        """
        Receive a bound parameter of type ``list[str]`` and convert to ``str``.

        Override the TypeDecorator method. The method is called at statement execution
        time and is passed the literal Python data value which is to be associated with
        a bound parameter in the statement.

        Notes
        -----
        Although this method must accept any type for compatibility with the SQLAlchemy
        `TypeDecorator` interface, it expects `value` to be a `list[str]`.

        Parameters
        ----------
        value : Any
            Data to operate upon. Must be a `list[str]` or coercible to one.

        dialect : Dialect
            The SQL Dialect in use.

        Returns
        -------
        str
            A single Python string representing a list of strings as comma separated
            values.
        """
        return "" if not value else ",".join(cast(list[str], value))

    def process_result_value(self, value: Any, dialect: Dialect) -> list[str]:
        """
        Receive a result-row column value of type ``str` and convert to ``list[str]``.

        This method is called at result fetching time and is passed the literal Python
        data value extracted from a database result row.

        Notes
        -----
        Although this method must accept any type for compatibility with the SQLAlchemy
        ``TypeDecorator`` interface, it expects ``value`` to be a `str`.

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
        return [] if value == "" else cast(str, value).split(",")


class Base(DeclarativeBase):
    """
    Subclass SQLAlchemy ``DeclarativeBase`` base class.

    All ORM Mapped classes should inherit from ``Base``. Tables can then be created
    with ``Base.metadata.create_all``.
    """

    # This is required to avoid mypy erros. See:
    # https://docs.sqlalchemy.org/en/20/changelog/migration_20.html#migration-to-2-0-step-six-add-allow-unmapped-to-explicitly-typed-orm-models
    __allow_unmapped__ = True


class Book(Base):
    """
    Readwise book as a SQL Alchemy ORM Mapped class.

    *WARNING* Using unvalidated API data directly with this class may result in
    unexpected behaviour and is not recommended. Validation is enforced in the Pydantic
    layer only. For example, this ORM class will accept null values for all fields -
    even those fields which should never be null. (Except the primary key which will not
    accept a null).

    Each class instance corresponds to a book dictionary from the Readwise
    'Highlight EXPORT' endpoint. "books" are parent object for all highlights, even
    those not sourced from books. Examples:

    +----------------+----------------------------------------------------------------+
    | Source         | Parent Object                                                  |
    +================+================================================================+
    | book           | book                                                           |
    +----------------+----------------------------------------------------------------+
    | twitter post   | A user's Tweets are considered a "book". E.g the book title    |
    |                | will be "Tweets from @<user>". Each saved post will be a       |
    |                | highlight in that 'book'.                                      |
    +----------------+----------------------------------------------------------------+
    | twitter thread | Individual threads are parents. The "book" title will be       |
    |                | truncated text from the first post.                            |
    +----------------+----------------------------------------------------------------+
    | podcast        | The podcast episode. (The podcast name is the 'author' field.) |
    +----------------+----------------------------------------------------------------+
    | article        | The article.                                                   |
    +----------------+----------------------------------------------------------------+
    | youtube        | Individual videos are treated as an article. (The channel name |
    |                | is the 'author' field).                                        |
    +----------------+----------------------------------------------------------------+

    (Twitter/X is referenced as Twitter, consistent with the Readwise API, March 2025).

    Attributes
    ----------
    user_book_id : int
        Primary key. Unique identifier sourced from Readwise.
    title: str
        The title of the parent object. E.g. Book title, twitter thread first post,
        podcast episode title etc.
    is_ deleted :
        User deleted book. Currently deleted books are stored with non-deleted books:
        handle downstream. No automation alters *highlights* of deleted books - it's
        assumed a deleted book's highlights will be fetched as "updated", with the
        highlights own 'is_deleted' status changed.
    author: str
        The article, tweet or article author, YouTube video creator, podcaster etc.
    readable_title : str
        The title, capitalized and tidied up. Reliably present (2993 out of 2993 sample
        user records).
    source : str
        A single word name for source of the object e.g ``Kindle``, ``twitter``,
        ``api_article`` etc.
    cover_image_url : str
        Link to the cover image. Set automatically by Readwise when highlighting via
        most methods. Seems to use native links where logical (e.g. Amazon, Twitter).
    unique_url : str
        Varies by input method. For example, ``"source": "web_clipper"`` may give a
        link to the original source document (i.e. the same link as ``source_url``).
        ``"source": "reader"`` may give the Readwise Reader link.
    summary : str
        Document summaries can be added in Readwise Reader.
    category : str
        A pre-defined Readwise category. Allowed values: ``books``, ``articles``,
        ``tweets``, ``podcasts``.
    document_note : str
        Can be added in Readwise Reader via the Notebook side panel.
    readwise_url : str
        The Readwise URL link to the "book"/parent object.
    source_url : str
        Link to the URL of the original source, if applicable. E.g. the Twitter account
        of the author, the original article etc.
    asin : str
        Not documented but Amazon Standard Identification Number. Only for Kindle
        highlights.

    batch_id:
        Foreign key linking the ``id`` of the associated ``ReadwiseBatch``.

    book_tags : list[BookTag]
        A list of user defined tags, applied to the parent object. These are distinct
        from highlight tags. (i.e. "arch_btw" could exist separately at a book and
        highlight level).
    highlights : list[Highlight]
        A list of highlights sourced from the book.
    batch : ReadwiseBatch
        The batch object the book was imported in.
    """

    __tablename__ = "books"

    user_book_id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[Optional[str]]
    is_deleted: Mapped[Optional[bool]]
    author: Mapped[Optional[str]]
    readable_title: Mapped[Optional[str]]
    source: Mapped[Optional[str]]
    cover_image_url: Mapped[Optional[str]]
    unique_url: Mapped[Optional[str]]
    summary: Mapped[Optional[str]]
    category: Mapped[Optional[str]]
    document_note: Mapped[Optional[str]]
    readwise_url: Mapped[Optional[str]]
    source_url: Mapped[Optional[str]]
    asin: Mapped[Optional[str]]

    batch_id: Mapped[int] = mapped_column(ForeignKey("readwise_batches.id"))

    book_tags: Mapped[list["BookTag"]] = relationship(back_populates="book")
    highlights: Mapped[list["Highlight"]] = relationship(back_populates="book")
    batch: Mapped["ReadwiseBatch"] = relationship(back_populates="books")

    def __repr__(self) -> str:
        return (
            f"Book(user_book_id={self.user_book_id!r}, title={self.title!r}, "
            f"highlights={len(self.highlights)})"
        )


class BookTag(Base):
    """
    Readwise book tag as a SQL Alchemy ORM Mapped class.

    *WARNING* Using unvalidated API data directly with this class may result in
    unexpected behaviour and is not recommended. Validation is enforced in the Pydantic
    layer only. For example, this ORM class will accept null values for all fields -
    even those fields which should never be null. (Except the primary key which will not
    accept a null).

    Each class instance corresponds to a book tags dictionary from the Readwise
    'Highlight EXPORT' endpoint.

    Attributes
    ----------
    id : int
        Primary key. Unique identifier sourced from Readwise.
    name : str
        The name of the tag. Each tag has an id and name. ``name``s are often common
        across tags/highlights but ``id`` is always unique. E.g. Many highlights may be
        tagged ``favourite`` but each ``favourite`` tag  will be associated with its own
        unique ``id``. Therefore, group by ``name`` for this attribute.

    book_id : int
        Foreign key linking the ``id`` of the associated ``Book``.
    batch_id : int
        Foreign key linking the `id` of the associated ``ReadwiseBatch``.

    book : Book
        The highlight object the tag is associated with.
    batch : ReadwiseBatch
        The batch object the tag was imported in.
    """

    __tablename__ = "book_tags"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(512))

    user_book_id: Mapped[int] = mapped_column(
        ForeignKey("books.user_book_id"), nullable=False
    )
    batch_id: Mapped[int] = mapped_column(
        ForeignKey("readwise_batches.id"), nullable=False
    )

    book: Mapped["Book"] = relationship(back_populates="book_tags")
    batch: Mapped["ReadwiseBatch"] = relationship(back_populates="book_tags")

    def __repr__(self) -> str:
        return f"BookTag(name={self.name!r}, id={self.id!r})"


class Highlight(Base):
    """
    Readwise highlight as a SQL Alchemy ORM Mapped class.

    *WARNING* Using unvalidated API data directly with this class may result in
    unexpected behaviour and is not recommended. Validation is enforced in the Pydantic
    layer only. For example, this ORM class will accept null values for all fields -
    even those fields which should never be null. (Except the primary key which will not
    accept a null).

    Each instance corresponds to a highlight dictionary from the Readwise 'Highlight
    EXPORT' endpoint. Highlights are text excerpts saved by the user from books,
    articles, or other sources.

    Attributes
    ----------
    id : int
        Primary key. Unique identifier sourced from Readwise.
    text : str
        The actual highlighted text content. Maximum length is 8191 characters.
    location : int
        Location if applicable. E.g. Kindle location, podcast/YouTube timestamp etc.
    location_type : str
        The type of location e.g. '``offset``, ``time_offset``, ``order``, ``location``,
        ``page`` (there may be others).
    note : str
        User notes added to the highlight.
    color : str
        Highlight color. Colors seen in user data: ``yellow``, ``pink``, ``orange``,
        ``blue``, ``purple``, ``green``.
    highlighted_at : datetime
        Time user made the highlight.
    created_at : datetime
        Time the highlight was added to the database.
    updated_at : datetime
        Time the highlight was edited (assumedly via the Readwise site or API).
    external_id :
        Seems to be the ID of highlight in the source service, where applicable.
        E.g. Readwise, Reader, ibooks, pocket, snipd, airr etc.
    end_location :
        Unknown. Always null in user data samples. Docs only show it as as null.
    url :
        Link to the highlight in the source service, where applicable. E.g. Readwise,
        Reader, ibooks, pocket, snipd, airr etc.
    is_favourite : bool
        User favourites highlight.
    is_discard : bool
        Is discarded by the user, presumably during "Readwise Daily Review".
    is_ deleted : bool
        User deleted highlight. Currently deleted highlights are stored with non-deleted
        highlights. Handle downstream.
    readwise_url :
        The Readwise URL link to the highlight.

    book_id : int
        Foreign key linking the `user_book_id` of the associated `Book`. ``book_id`` is
        the Readwise key name, retained for consistency with the Readwise API.
    batch_id : int
        Foreign key linking the `id` of the associated `ReadwiseBatch`.

    book : Book
        The book the highlight belongs to.
    tags : list[HighlightTag]
        Tags the user has assigned to this highlight.
    batch : ReadwiseBatch
        The batch object the highlight was imported in.
    """

    __tablename__ = "highlights"

    id: Mapped[int] = mapped_column(primary_key=True)
    text: Mapped[str] = mapped_column(String(8191))
    location: Mapped[Optional[int]]
    location_type: Mapped[Optional[str]]
    note: Mapped[Optional[str]]
    color: Mapped[Optional[str]]
    highlighted_at: Mapped[Optional[datetime]]
    created_at: Mapped[Optional[datetime]]
    updated_at: Mapped[Optional[datetime]]
    external_id: Mapped[Optional[str]]
    end_location: Mapped[Optional[int]]
    url: Mapped[Optional[str]]
    is_favorite: Mapped[Optional[bool]]
    is_discard: Mapped[Optional[bool]]
    is_deleted: Mapped[Optional[bool]]
    readwise_url: Mapped[Optional[str]]

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

    *WARNING* Using unvalidated API data directly with this class may result in
    unexpected behaviour and is not recommended. Validation is enforced in the Pydantic
    layer only. For example, this ORM class will accept null values for all fields -
    even those fields which should never be null. (Except the primary key which will not
    accept a null).

    Each class instance corresponds to a highlight tags dictionary from the Readwise
    'Highlight EXPORT' endpoint.

    Attributes
    ----------
    id : int
        Primary key. Unique identifier sourced from Readwise.
    name : str
        The name of the tag. Each tag has an id and name. ``name``s are often common
        across tags/highlights but ``id`` is always unique. E.g. Many highlights may be
        tagged ``favourite`` but each ``favourite`` tag  will be associated with its own
        unique ``id``. Therefore, group by ``name`` for this attribute.

    highlight_id : int
        Foreign key linking the ``id`` of the associated ``Highlight``.
    batch_id : int
        Foreign key linking the `id` of the associated `ReadwiseBatch`.

    highlight : Highlight
        The highlight object the tag is associated with.
    batch : ReadwiseBatch
        The batch object the tag was imported in.
    """

    __tablename__ = "highlight_tags"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(512))

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

    This is not API data, therefore validation is performed here in the ORM layer.

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
    book_tags: Mapped[list["BookTag"]] = relationship(back_populates="batch")
    highlights: Mapped[list["Highlight"]] = relationship(back_populates="batch")
    highlight_tags: Mapped[list["HighlightTag"]] = relationship(back_populates="batch")

    def __repr__(self) -> str:
        parts = [f"ReadwiseBatch(id={self.id!r}"]
        parts.append(f"books={len(self.books)}")
        parts.append(f"highlights={len(self.highlights)}")
        parts.append(f"book_tags={len(self.book_tags)}")
        parts.append(f"highlight_tags={len(self.highlight_tags)}")
        if self.start_time:
            parts.append(f"start={self.start_time.isoformat()}")
        if self.end_time:
            parts.append(f"end={self.end_time.isoformat()}")
        if self.database_write_time:
            parts.append(f"write={self.database_write_time.isoformat()}")
        return ", ".join(parts) + ")"
