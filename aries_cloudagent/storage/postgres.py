"""Postgres implementation of BaseStorage interface."""

import asyncio
import json
from typing import Mapping, Sequence
from urllib.parse import urlparse

import asyncpg

from ..wallet.base import BaseWallet

from .base import BaseStorage, BaseStorageRecordSearch
from .error import (
    StorageError,
    StorageDuplicateError,
    StorageNotFoundError,
    StorageSearchError,
)
from .record import StorageRecord, validate_record

KEEPALIVE = 10


class ConnectionContext:
    """Simple replacement for contextlib.asynccontextmanager."""

    def __init__(self, pool_mgr: "ConnectionPoolManager"):
        self.conn: asyncpg.Connection = None
        self.pool = None
        self.pool_mgr = pool_mgr
        self.refs = 0

    async def open(self) -> asyncpg.Connection:
        if not self.conn:
            pool = await self.pool_mgr.pool()
            self.conn = await pool.acquire()
        self.refs += 1
        return self.conn

    async def close(self):
        if self.refs:
            self.refs -= 1
        if not self.refs:
            pool = await self.pool_mgr.pool()
            await pool.release(self.conn)
            self.conn = None

    async def __aenter__(self):
        return await self.open()

    async def __aexit__(self, err_type, err_val, err_tb):
        await self.close()


class ConnectionPoolManager:
    def __init__(self, settings: dict):
        """Initialize the connection handler."""
        self._config: dict = None
        self._creds: dict = None
        self._init_lock = asyncio.Lock()
        self._pool: asyncpg.pool.Pool = None
        self.load_config(settings)

    def load_config(self, settings: dict):
        """Check that the config parameters are provided."""
        name = settings.get("wallet.name")
        config = json.loads(settings.get("wallet.storage_config") or "{}")
        creds = json.loads(settings.get("wallet.storage_creds") or "{}")
        if not name:
            raise StorageError("Missing postgres database name")
        if not config:
            raise StorageError("Missing postgres config")
        if not config.get("url"):
            raise StorageError("Missing postgres URL")
        config["name"] = name
        if (
            not creds
            or not creds.get("admin_account")
            or not creds.get("admin_password")
        ):
            raise StorageError("Missing postgres credentials")
        self._config = config
        self._creds = creds

    async def pool(self) -> asyncpg.pool.Pool:
        if not self._pool:
            async with self._init_lock:
                if not self._pool:
                    url = self._config["url"]
                    if "://" not in url:
                        url = f"http://{url}"
                    parts = urlparse(url)
                    self._pool = await asyncpg.create_pool(
                        host=parts.hostname,
                        port=parts.port or 5432,
                        user=self._creds["admin_account"],
                        password=self._creds["admin_password"],
                        database=self._config["name"],
                    )
                    # except asyncpg.InvalidCatalogNameError: connect to template1
        return self._pool

    def connection(self) -> asyncpg.Connection:
        """Open a connection handle."""
        return ConnectionContext(self)


class PostgresStorage(BaseStorage):
    """Postgres Non-Secrets interface."""

    def __init__(self, _wallet: BaseWallet, settings: dict):
        """
        Initialize a `PostgresStorage` instance.

        Args:
            _wallet: The wallet implementation to use
            settings: Context configuration settings

        """
        self._pool_mgr = ConnectionPoolManager(settings)

    @property
    def pool_mgr(self) -> ConnectionPoolManager:
        return self._pool_mgr

    async def init_storage(self, reset: bool = False):
        """Initialize non-secrets tables."""
        create_sql = """
            CREATE TABLE IF NOT EXISTS unenc_storage (
                record_id varchar(36),
                record_type text,
                record_value text,
                PRIMARY KEY (record_id)
            );
            CREATE TABLE IF NOT EXISTS unenc_storage_tags (
                record_id varchar(36) REFERENCES unenc_storage(record_id)
                    ON DELETE CASCADE,
                tag_name text,
                tag_value text,
                PRIMARY KEY (record_id, tag_name)
            );
        """
        if reset:
            create_sql += "DELETE FROM unenc_storage;"
        # FIXME unique tag name restriction
        async with self.pool_mgr.connection() as conn:
            await conn.execute(create_sql)

    async def add_record(self, record: StorageRecord):
        """
        Add a new record to the store.

        Args:
            record: `StorageRecord` to be stored

        """
        validate_record(record)
        insert_sql = (
            "INSERT INTO unenc_storage "
            "(record_id, record_type, record_value) VALUES ($1, $2, $3)"
        )
        insert_tag_sql = (
            "INSERT INTO unenc_storage_tags "
            "(record_id, tag_name, tag_value) VALUES ($1, $2, $3)"
        )
        try:
            async with self.pool_mgr.connection() as conn:
                async with conn.transaction():
                    await conn.execute(insert_sql, record.id, record.type, record.value)
                    if record.tags:
                        for tag_name, tag_val in record.tags.items():
                            await conn.execute(
                                insert_tag_sql, record.id, tag_name, tag_val
                            )
        except asyncpg.UniqueViolationError as e:
            raise StorageDuplicateError(
                "Duplicate record ID: {}".format(record.id)
            ) from e

    async def get_record(self, record_type: str, record_id: str) -> StorageRecord:
        """
        Fetch a record from the store by type and ID.

        Args:
            record_type: The record type
            record_id: The record id

        Returns:
            A `StorageRecord` instance

        Raises:
            StorageError: If the record is not provided
            StorageError: If the record ID not provided
            StorageNotFoundError: If the record is not found
            StorageError: If record not found

        """
        if not record_type:
            raise StorageError("Record type not provided")
        if not record_id:
            raise StorageError("Record ID not provided")
        select_sql = (
            "SELECT record_value FROM unenc_storage "
            "WHERE record_id=$1 AND record_type=$2"
        )
        select_tag_sql = (
            "SELECT tag_name, tag_value FROM unenc_storage_tags WHERE record_id=$1"
        )
        value = None
        tags = {}
        async with self.pool_mgr.connection() as conn:
            value_row = await conn.fetchrow(select_sql, record_id, record_type)
            if not value_row:
                raise StorageNotFoundError("Record not found: {}".format(record_id))
            value = value_row["record_value"]
            for tag_row in await conn.fetch(select_tag_sql, record_id):
                tags[tag_row["tag_name"]] = tag_row["tag_value"]
        # print("retrieved", value, tags)
        return StorageRecord(type=record_type, id=record_id, value=value, tags=tags)

    async def update_record_value(self, record: StorageRecord, value: str):
        """
        Update an existing stored record's value.

        Args:
            record: `StorageRecord` to update
            value: The new value

        Raises:
            StorageNotFoundError: If record not found
            StorageError: If a libindy error occurs

        """
        validate_record(record)
        update_sql = (
            "UPDATE unenc_storage SET record_value=$1 "
            "WHERE record_id=$2 AND record_type=$3 "
            "RETURNING 1"
        )
        async with self.pool_mgr.connection() as conn:
            updated = await conn.fetchval(update_sql, value, record.id, record.type)
            if not updated:
                raise StorageNotFoundError("Record not found: {}".format(record.id))

    async def update_record_tags(self, record: StorageRecord, tags: Mapping):
        """
        Update an existing stored record's tags.

        Args:
            record: `StorageRecord` to update
            tags: New tags

        Raises:
            StorageNotFoundError: If record not found
            StorageError: If a libindy error occurs

        """
        validate_record(record)
        select_tags_sql = (
            "SELECT tag_name, tag_value FROM unenc_storage_tags " "WHERE record_id=$1"
        )
        insert_tags_sql = (
            "INSERT INTO unenc_storage_tags (record_id, tag_name, tag_value)"
            "VALUES ($1, $2, $3)"
        )
        delete_tags_sql = (
            "DELETE FROM unenc_storage_tags "
            "WHERE record_id=$1 AND tag_name=ANY($2::text[]) "
            "RETURNING tag_name"
        )

        # check existence of record first (otherwise no exception thrown)
        await self.get_record(record.type, record.id)

        async with self.pool_mgr.connection() as conn:
            async with conn.transaction():
                exist_tags = {}
                for tag_row in await conn.fetch(select_tags_sql, record.id):
                    exist_tags[tag_row["tag_name"]] = tag_row["tag_value"]
                remove_tags = set(
                    tag_name
                    for tag_name in exist_tags
                    if tag_name not in tags or tags[tag_name] != exist_tags[tag_name]
                )
                if remove_tags:
                    await conn.execute(delete_tags_sql, record.id, remove_tags)
                insert_tags = {
                    tag_name: tags[tag_name]
                    for tag_name in tags
                    if (tag_name not in exist_tags or tag_name in remove_tags)
                }
                for tag_name, tag_value in insert_tags.items():
                    await conn.execute(insert_tags_sql, record.id, tag_name, tag_value)

    async def delete_record_tags(
        self, record: StorageRecord, tags: (Sequence, Mapping)
    ):
        """
        Update an existing stored record's tags.

        Args:
            record: `StorageRecord` to delete
            tags: Tags

        """
        validate_record(record)

        delete_tags_sql = (
            "DELETE FROM unenc_storage_tags "
            "WHERE record_id=$1 AND tag_name=ANY($2::text[]) "
            "RETURNING tag_name"
        )

        if tags:
            async with self.pool_mgr.connection() as conn:
                removed = await conn.fetchval(delete_tags_sql, record.id, list(tags))
                if not removed:
                    raise StorageNotFoundError("Record not found: {}".format(record.id))

    async def delete_record(self, record: StorageRecord):
        """
        Delete a record.

        Args:
            record: `StorageRecord` to delete

        Raises:
            StorageNotFoundError: If record not found
            StorageError: If a libindy error occurs

        """
        validate_record(record)
        delete_sql = (
            "DELETE FROM unenc_storage WHERE record_id=$1 AND record_type=$2 "
            "RETURNING 1"
        )
        async with self.pool_mgr.connection() as conn:
            removed = await conn.fetchval(delete_sql, record.id, record.type)
            if not removed:
                raise StorageNotFoundError("Record not found: {}".format(record.id))

    def search_records(
        self, type_filter: str, tag_query: Mapping = None, page_size: int = None
    ) -> "PostgresStorageRecordSearch":
        """
        Search stored records.

        Args:
            type_filter: Filter string
            tag_query: Tags to query
            page_size: Page size

        Returns:
            An instance of `BaseStorageRecordSearch`

        """
        return PostgresStorageRecordSearch(self, type_filter, tag_query, page_size)


def tag_value_sql(tag_name: str, match: dict, idx=1) -> (str, list):
    """Match a single tag against a tag subquery."""
    if len(match) != 1:
        raise StorageSearchError("Unsupported subquery: {}".format(match))
    op = next(iter(match.keys()))
    cmp_val = match[op]
    if op == "$in":
        if not isinstance(cmp_val, list):
            raise StorageSearchError("Expected list for $in value")
        sql_op = "IN"
    else:
        if not isinstance(cmp_val, str):
            raise StorageSearchError("Expected string for filter value")
        if op == "$eq":
            sql_op = "="
        elif op == "$neq":
            sql_op = "!="
        elif op == "$gt":
            sql_op = ">"
        elif op == "$gte":
            sql_op = ">="
        elif op == "$lt":
            sql_op = "<"
        elif op == "$lte":
            sql_op = "<="
        # elif op == "$like":  NYI
        else:
            raise StorageSearchError("Unsupported match operator: ".format(op))
    sql = (
        "EXISTS (SELECT 1 FROM unenc_storage_tags WHERE "
        f"tag_name = ${idx}::text AND tag_value {sql_op} ${idx+1}::text "
        "AND record_id=unenc_storage.record_id)"
    )
    return sql, [tag_name, cmp_val]


def tag_query_sql(tag_query: dict, idx=1) -> (str, list):
    """Match simple tag filters (string values)."""
    args = []
    clauses = []
    if tag_query:
        for k, v in tag_query.items():
            if k == "$or":
                if not isinstance(v, list):
                    raise StorageSearchError("Expected list for $or filter value")
                cl_opts = []
                for opt in v:
                    cl_sql, cl_args = tag_query_sql(opt, idx)
                    args.extend(cl_args)
                    cl_opts.append(cl_sql)
                    idx += len(cl_args)
                # FIXME - add brackets
                clauses.append(" OR ".join(cl_opts))
            elif k == "$not":
                if not isinstance(v, dict):
                    raise StorageSearchError("Expected dict for $not filter value")
                cl_sql, cl_args = tag_query_sql(v, idx)
                args.extend(cl_args)
                idx += len(cl_args)
                clauses.append(f"NOT {cl_sql}")
            elif k[0] == "$":
                raise StorageSearchError("Unexpected filter operator: {}".format(k))
            elif isinstance(v, str):
                sql, cl_args = tag_value_sql(k, {"$eq": v}, idx)
                args.extend(cl_args)
                idx += len(cl_args)
                clauses.append(sql)
            elif isinstance(v, dict):
                # chk = tag_value_sql(k, tags.get(k), v)
                raise StorageSearchError("??")
            else:
                raise StorageSearchError(
                    "Expected string or dict for filter value, got {}".format(v)
                )
    sql = " AND ".join(clauses)
    return sql, args


class PostgresStorageRecordSearch(BaseStorageRecordSearch):
    """Represent an active stored records search."""

    def __init__(
        self,
        store: PostgresStorage,
        type_filter: str,
        tag_query: Mapping,
        page_size: int = None,
    ):
        """
        Initialize a `PostgresStorageRecordSearch` instance.

        Args:
            store: `BaseStorage` to search
            type_filter: Filter string
            tag_query: Tags to search
            page_size: Size of page to return

        """
        super(PostgresStorageRecordSearch, self).__init__(
            store, type_filter, tag_query, page_size
        )
        self._conn: asyncpg.Connection = None
        self._conn_ctx: ConnectionContext = None
        self._handle: asyncpg.cursor.Cursor = None
        self._txn: asyncpg.transaction.Transaction = None

    @property
    def opened(self) -> bool:
        """
        Accessor for open state.

        Returns:
            True if opened, else False

        """
        return self._handle is not None

    @property
    def handle(self):
        """
        Accessor for search handle.

        Returns:
            The handle

        """
        return self._handle

    async def fetch(self, max_count: int) -> Sequence[StorageRecord]:
        """
        Fetch the next list of results from the store.

        Args:
            max_count: Max number of records to return

        Returns:
            A list of `StorageRecord`

        Raises:
            StorageSearchError: If the search query has not been opened

        """
        if not self.opened:
            raise StorageSearchError("Search query has not been opened")
        select_tag_sql = (
            "SELECT tag_name, tag_value FROM unenc_storage_tags WHERE record_id=$1"
        )
        rows = await self._handle.fetch(max_count)
        ret = []
        for row in rows:
            tag_rows = await self._conn.fetch(select_tag_sql, row["record_id"])
            tags = {}
            for tag_row in tag_rows:
                tags[tag_row["tag_name"]] = tag_row["tag_value"]
            ret.append(
                StorageRecord(
                    type=row["record_type"],
                    id=row["record_id"],
                    value=row["record_value"],
                    tags=tags,
                )
            )
        return ret

    async def open(self):
        """Start the search query."""
        select_sql = (
            "SELECT record_id, record_type, record_value "
            "FROM unenc_storage WHERE record_type=$1"
        )
        tag_filter_sql, tag_args = tag_query_sql(self.tag_query, 2)
        if tag_filter_sql:
            select_sql += f" AND {tag_filter_sql}"
        # print("query", select_sql)
        self._conn_ctx = self._store.pool_mgr.connection()
        self._conn = await self._conn_ctx.open()
        self._txn = self._conn.transaction()
        await self._txn.start()
        self._handle = await self._conn.cursor(select_sql, self.type_filter, *tag_args)

    async def close(self):
        """Dispose of the search query."""
        if self._handle:
            self._handle = None
        if self._conn:
            await self._txn.commit()
            await self._conn_ctx.close()
            self._conn = None
            self._conn_ctx = None

    # FIXME  def __del__(self):
