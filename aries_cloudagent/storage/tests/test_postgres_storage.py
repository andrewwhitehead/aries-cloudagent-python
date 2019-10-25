from collections import OrderedDict
import pytest

from aries_cloudagent.storage.error import (
    StorageDuplicateError,
    StorageError,
    StorageNotFoundError,
    StorageSearchError,
)

from aries_cloudagent.storage.record import StorageRecord

from aries_cloudagent.storage.postgres import PostgresStorage, tag_query_sql

from . import test_basic_storage


@pytest.fixture()
async def store():
    store = PostgresStorage(
        None,
        {
            "wallet.name": "test-db",
            "wallet.storage_config": '{"url": "192.168.65.3:5432"}',
            "wallet.storage_creds": '{"account":"postgres","password":"mysecretpassword","admin_account":"postgres","admin_password":"mysecretpassword"}',
        },
    )
    await store.init_storage(True)
    yield store


@pytest.mark.postgres
class TestPostgresStorage(test_basic_storage.TestBasicStorage):
    pass


class TestTagQuerySql:
    def test_name_filter(self):
        query = {"tag": "value"}
        sql, args = tag_query_sql(query)
        assert sql == "exist(tags, $1) AND tags -> $1 = $2"
        assert args == ["tag", "value"]

    def test_negate_name_filter(self):
        query = {"$not": {"tag": "value"}}
        sql, args = tag_query_sql(query)
        assert sql == "NOT (exist(tags, $1) AND tags -> $1 = $2)"
        assert args == ["tag", "value"]

    def test_inc_filter(self):
        query = OrderedDict([("tag1", "value1"), ("tag2", "value2")])
        sql, args = tag_query_sql(query)
        assert (
            sql
            == "exist(tags, $1) AND tags -> $1 = $2 AND exist(tags, $3) AND tags -> $3 = $4"
        )
        assert args == ["tag1", "value1", "tag2", "value2"]

    def test_alt_filter(self):
        query = {"$or": [{"tag1": "value1"}, {"tag2": "value2"}]}
        sql, args = tag_query_sql(query)
        assert (
            sql
            == "(exist(tags, $1) AND tags -> $1 = $2) OR (exist(tags, $3) AND tags -> $3 = $4)"
        )
        assert args == ["tag1", "value1", "tag2", "value2"]
