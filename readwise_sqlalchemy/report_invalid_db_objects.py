"""
Report invalid database objects (books, highlights, book_tags) in a readable format.
"""
from readwise_sqlalchemy import models
from readwise_sqlalchemy.config import USER_CONFIG
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

def report_invalid_db_objects():
    db_url = f"sqlite:///{USER_CONFIG.db_path}"
    engine = create_engine(db_url)
    session = Session(engine)
    try:
        books = session.query(models.Book).all()
        for book in books:
            invalid_children = []
            for hl in book.highlights:
                if not hl.validated:
                    invalid_children.append(("highlight", hl))
            for tag in book.book_tags:
                if not tag.validated:
                    invalid_children.append(("book_tag", tag))
            if invalid_children:
                print(f"\n❌ Book: '{book.title}'")
                print(f"  - Book valid: {'✅' if book.validated else '❌'}")
                print(f"  - Children valid: ❌")
                for kind, obj in invalid_children:
                    if kind == "highlight":
                        print(f"    ⤷ Highlight ID {obj.id}: {obj.validation_errors}")
                    elif kind == "book_tag":
                        print(f"    ⤷ BookTag ID {obj.id}: {obj.validation_errors}")
    finally:
        session.close()
