"""Record instance stored and searchable by BaseStorage implementation."""

from collections import namedtuple
from uuid import uuid4

from .error import StorageError


class StorageRecord(namedtuple("StorageRecord", "type value tags id")):
    """Storage record class."""

    __slots__ = ()

    def __new__(cls, type, value, tags: dict = None, id: str = None):
        """Initialize some defaults on record."""
        if not id:
            id = uuid4().hex
        if not tags:
            tags = {}
        return super(cls, StorageRecord).__new__(cls, type, value, tags, id)

    def validate(self):
        """Check that the record is capable of being persisted."""
        if not self.id:
            raise StorageError("Record has no ID")
        if not self.type:
            raise StorageError("Record has no type")
        if not self.value:
            raise StorageError("Record must have a non-empty value")


def validate_record(record: StorageRecord):
    """Check that a record is non-empty has has the required properties."""
    if not record:
        raise StorageError("No record provided")
    record.validate()
