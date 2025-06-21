"""
Report invalid database objects (books, highlights, book_tags) in a readable format.
"""

from typing import Optional

from rich.progress import Progress

from readwise_sqlalchemy.config import UserConfig, fetch_user_config
from readwise_sqlalchemy.db_operations import get_session
from readwise_sqlalchemy.models import Book, BookTag, Highlight, HighlightTag


def list_invalid_db_objects(user_config: Optional[UserConfig] = None) -> None:
    """
    Report invalid database objects (books, highlights, book_tags) in a readable format.
    """
    if user_config is None:
        user_config = fetch_user_config()

    session = get_session(user_config.db_path)

    try:
        books = session.query(Book).all()
        total_books = len(books)

        # Add rich progress bar.
        with Progress() as progress:
            book_task = progress.add_task(
                "[cyan]Validating books...", total=total_books
            )
            for book in books:
                invalid_children: list[
                    tuple[str, BookTag | Highlight | HighlightTag]
                ] = []
                for highlight in book.highlights:
                    if not highlight.validated:
                        invalid_children.append(("highlight", highlight))

                    for highlight_tag in highlight.tags:
                        if not highlight_tag.validated:
                            invalid_children.append(("highlight_tag", highlight_tag))

                for book_tag in book.book_tags:
                    if not book_tag.validated:
                        invalid_children.append(("book_tag", book_tag))

                if invalid_children or not book.validated:
                    print(f"\n❌ Book: '{book.title}'")
                    print(f"  - Book valid: {'✅' if book.validated else '❌'}")
                    print("  - Children valid: ❌")
                    for kind, obj in invalid_children:
                        if kind == "highlight":
                            print(
                                f"    ⤷ Highlight ID {obj.id}: {obj.validation_errors}"
                            )
                        elif kind == "book_tag":
                            print(f"    ⤷ BookTag ID {obj.id}: {obj.validation_errors}")
                        elif kind == "highlight_tag":
                            print(
                                f"    ⤷ HighlightTag ID {obj.id}: {obj.validation_errors}"
                            )
                progress.update(book_task, advance=1)
        print("\nAll books processed.")
    finally:
        session.close()
