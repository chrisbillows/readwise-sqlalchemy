from typing import List, Optional

from sqlalchemy import ForeignKey, String, create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship

# --- Base setup ---


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user_account"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    fullname: Mapped[Optional[str]]

    addresses: Mapped[List["Address"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"


class AddressFieldsMixin:
    id: Mapped[int] = mapped_column(primary_key=True)
    email_address: Mapped[str]
    user_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))


class Address(Base, AddressFieldsMixin):
    __tablename__ = "address"
    __mapper_args__ = {"concrete": True}

    user: Mapped["User"] = relationship(back_populates="addresses")


class AddressVersion(Base, AddressFieldsMixin):
    __tablename__ = "address_version"
    __mapper_args__ = {"concrete": True}

    new_field: Mapped[str] = mapped_column(String(30))
    original_address_id: Mapped[int] = mapped_column(ForeignKey("address.id"))


# --- Setup DB engine and create tables ---

engine = create_engine("sqlite://", echo=True)
Base.metadata.create_all(engine)

session = Session(engine)


# --- Insert example data ---

user = User(name="Chris Billows")
session.add(user)
session.commit()

# Original address
addr = Address(email_address="chris.old@example.com", user_id=user.id)
session.add(addr)
session.commit()

# Versioned copy of the address
addr_version = AddressVersion(
    email_address=addr.email_address,
    user_id=addr.user_id,
    new_field="Some different data",
    original_address_id=addr.id,
)
session.add(addr_version)
session.commit()


# --- Peek at tables ---

inspector = inspect(engine)
tables = inspector.get_table_names()

users = session.execute(text("SELECT * FROM user_account")).all()
addresses = session.execute(text("SELECT * FROM address")).all()
address_versions = session.execute(text("SELECT * FROM address_version")).all()

breakpoint()

"""
>>> cols = inspector.get_columns("address_version")
>>> for x in cols:
...   print(x['name'])
...
new_field
original_address_id
id
email_address
user_id

# So columns via the mixin are added AFTER the new columns - but it doesn't really matter.
"""
