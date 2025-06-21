"""
Report invalid database objects (books, highlights, book_tags) in a readable format.
"""
from rich.progress import Progress

from readwise_sqlalchemy import models
from readwise_sqlalchemy.config import UserConfig, fetch_user_config
from readwise_sqlalchemy.db_operations import get_session


def list_invalid_db_objects(user_config: UserConfig = fetch_user_config()) -> None:
    """
    Report invalid database objects (books, highlights, book_tags) in a readable format.
    """
    session = get_session(user_config.db_path)
    try:
        books = session.query(models.Book).all()
        total_books = len(books)
        # Add rich progress bar.
        with Progress() as progress:
            book_task = progress.add_task(
                "[cyan]Validating books...", total=total_books
            )
            for book in books:
                invalid_children = []
                for hl in book.highlights:
                    if not hl.validated:
                        invalid_children.append(("highlight", hl))

                for tag in book.book_tags:
                    if not tag.validated:
                        invalid_children.append(("book_tag", tag))

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
                progress.update(book_task, advance=1)
        print("\nAll books processed.")
    finally:
        session.close()
