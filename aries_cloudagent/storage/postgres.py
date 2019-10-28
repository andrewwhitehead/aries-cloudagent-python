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


class ConnectionPoolManager:
    """Manager for the postgres connection pool."""

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

    @property
    def parsed_url(self):
        """Accessor for the parsed database URL."""
        url = self._config["url"]
        if "://" not in url:
            url = f"http://{url}"
        return urlparse(url)

    @property
    def pool(self) -> asyncpg.pool.Pool:
        """Accessor for the connection pool instance."""
        if not self._pool:
            parts = self.parsed_url
            self._pool = asyncpg.create_pool(
                host=parts.hostname,
                port=parts.port or 5432,
                user=self._creds["admin_account"],
                password=self._creds["admin_password"],
                database=self._config["name"],
                min_size=1,
                max_size=5,
                init=self.init_connection,
            )
        return self._pool

    async def create_database(self):
        """Create the database."""
        parts = self.parsed_url
        conn = await asyncpg.connect(
            host=parts.hostname,
            port=parts.port or 5432,
            user=self._creds["admin_account"],
            password=self._creds["admin_password"],
            database="template1",
        )
        await conn.execute(f"CREATE DATABASE \"{self._config['name']}\"")
        await conn.close()

    async def setup(self):
        """Set up the connection pool, creating the database if necessary."""
        try:
            await self.pool
        except asyncpg.exceptions.InvalidCatalogNameError:
            await self.create_database()
            await self.pool

    async def init_connection(self, conn: asyncpg.Connection):
        """Init the connection by registering needed codecs."""
        await conn.execute("CREATE EXTENSION IF NOT EXISTS hstore")
        await conn.set_builtin_type_codec("hstore", codec_name="pg_contrib.hstore")

    @property
    def connection(self) -> asyncpg.pool.PoolAcquireContext:
        """Return a connection handle."""
        return self.pool.acquire()

    async def release(self, conn: asyncpg.Connection):
        """Release a previously-acquired connection."""
        if conn:
            await self.pool.release(conn)


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
        """Accessor for the `ConnectionPoolManager` instance."""
        return self._pool_mgr

    async def init_storage(self, reset: bool = False):
        """Initialize non-secrets tables."""
        create_sql = """
            CREATE TABLE IF NOT EXISTS unenc_storage (
                record_id VARCHAR(36),
                record_type TEXT,
                record_value TEXT,
                tags HSTORE,
                PRIMARY KEY (record_id)
            );
            CREATE INDEX IF NOT EXISTS unenc_did ON unenc_storage (
                (tags -> 'did')
            ) WHERE exist(tags, 'did');
            CREATE INDEX IF NOT EXISTS unenc_initiator ON unenc_storage (
                (tags -> 'initiator')
            ) WHERE exist(tags, 'initiator');
            CREATE INDEX IF NOT EXISTS unenc_thread_id ON unenc_storage (
                (tags -> 'thread_id')
            ) WHERE exist(tags, 'thread_id');
            CREATE INDEX IF NOT EXISTS unenc_my_did ON unenc_storage (
                (tags -> 'my_did')
            ) WHERE exist(tags, 'my_did');
            CREATE INDEX IF NOT EXISTS unenc_their_did ON unenc_storage (
                (tags -> 'their_did')
            ) WHERE exist(tags, 'their_did');
            CREATE INDEX IF NOT EXISTS unenc_state ON unenc_storage (
                (tags -> 'state')
            ) WHERE exist(tags, 'state');
        """
        if reset:
            create_sql += "DELETE FROM unenc_storage;"
        await self.pool_mgr.setup()
        async with self.pool_mgr.connection as conn:
            await conn.execute(create_sql)

    async def add_record(self, record: StorageRecord):
        """
        Add a new record to the store.

        Args:
            record: `StorageRecord` to be stored

        """
        validate_record(record)
        insert_sql = """
            INSERT INTO unenc_storage
            (record_id, record_type, record_value, tags) VALUES ($1, $2, $3, $4)
        """
        try:
            async with self.pool_mgr.connection as conn:
                await conn.execute(
                    insert_sql, record.id, record.type, record.value, record.tags
                )
        except asyncpg.UniqueViolationError as e:
            raise StorageDuplicateError(
                "Duplicate record ID: {}".format(record.id)
            ) from e

    async def get_record(
        self, record_type: str, record_id: str, options: Mapping = None
    ) -> StorageRecord:
        """
        Fetch a record from the store by type and ID.

        Args:
            record_type: The record type
            record_id: The record id
            options: A dictionary of backend-specific options

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
        select_sql = """
            SELECT record_value, tags
            FROM unenc_storage WHERE record_id=$1 and record_type=$2
        """
        value = None
        tags = {}
        async with self.pool_mgr.connection as conn:
            row = await conn.fetchrow(select_sql, record_id, record_type)
            if not row:
                raise StorageNotFoundError("Record not found: {}".format(record_id))
            value = row["record_value"]
            tags = row["tags"] or {}
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
        update_sql = """
            UPDATE unenc_storage SET record_value=$1
            WHERE record_id=$2 AND record_type=$3
            RETURNING 1
        """
        async with self.pool_mgr.connection as conn:
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
        update_tags_sql = (
            "UPDATE unenc_storage SET tags=$1 WHERE record_id=$2 RETURNING 1"
        )

        async with self.pool_mgr.connection as conn:
            updated = await conn.fetchval(update_tags_sql, tags or {}, record.id)
            if not updated:
                raise StorageNotFoundError("Record not found: {}".format(record.id))

    async def delete_record_tags(
        self, record: StorageRecord, tags: (Sequence, Mapping)
    ):
        """
        Remove an existing stored record's tags.

        Args:
            record: `StorageRecord` to delete
            tags: Tags

        """
        validate_record(record)
        delete_tags_sql = """
            UPDATE unenc_storage SET tags = tags - $1::text[]
            WHERE record_id=$2 RETURNING 1
        """

        if tags:
            async with self.pool_mgr.connection as conn:
                removed = await conn.fetchval(delete_tags_sql, list(tags, record.id))
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
        delete_sql = """
            DELETE FROM unenc_storage WHERE record_id=$1 AND record_type=$2
            RETURNING 1
        """
        async with self.pool_mgr.connection as conn:
            removed = await conn.fetchval(delete_sql, record.id, record.type)
            if not removed:
                raise StorageNotFoundError("Record not found: {}".format(record.id))

    def search_records(
        self,
        type_filter: str,
        tag_query: Mapping = None,
        page_size: int = None,
        options: Mapping = None,
    ) -> "PostgresStorageRecordSearch":
        """
        Search stored records.

        Args:
            type_filter: Filter string
            tag_query: Tags to query
            page_size: Page size
            options: Dictionary of backend-specific options

        Returns:
            An instance of `BaseStorageRecordSearch`

        """
        return PostgresStorageRecordSearch(
            self, type_filter, tag_query, page_size, options
        )


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
    sql = f"exist(tags, ${idx}) AND tags -> ${idx} {sql_op} ${idx+1}"
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
                if len(cl_opts) == 1:
                    clauses.append(cl_opts[0])
                else:
                    clauses.append(" OR ".join(f"({c})" for c in cl_opts))
            elif k == "$not":
                if not isinstance(v, dict):
                    raise StorageSearchError("Expected dict for $not filter value")
                cl_sql, cl_args = tag_query_sql(v, idx)
                args.extend(cl_args)
                idx += len(cl_args)
                clauses.append(f"NOT ({cl_sql})")
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
        options: Mapping = None,
    ):
        """
        Initialize a `PostgresStorageRecordSearch` instance.

        Args:
            store: `BaseStorage` to search
            type_filter: Filter string
            tag_query: Tags to search
            page_size: Size of page to return
            options: Dictionary of backend-specific options

        """
        super(PostgresStorageRecordSearch, self).__init__(
            store, type_filter, tag_query, page_size, options
        )
        self._conn: asyncpg.Connection = None
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
        rows = await self._handle.fetch(max_count)
        ret = []
        for row in rows:
            ret.append(
                StorageRecord(
                    type=row["record_type"],
                    id=row["record_id"],
                    value=row["record_value"],
                    tags=row["tags"] or {},
                )
            )
        return ret

    async def open(self):
        """Start the search query."""
        select_sql = """
            SELECT record_id, record_type, record_value, tags
            FROM unenc_storage WHERE record_type=$1
        """
        tag_filter_sql, tag_args = tag_query_sql(self.tag_query, 2)
        if tag_filter_sql:
            select_sql += f" AND {tag_filter_sql}"
        # print("query", select_sql)
        self._conn = await self._store.pool_mgr.connection
        self._txn = self._conn.transaction()
        await self._txn.start()
        self._handle = await self._conn.cursor(select_sql, self.type_filter, *tag_args)

    async def close(self):
        """Dispose of the search query."""
        if self._handle:
            self._handle = None
        if self._conn:
            await self._txn.commit()
            self._txn = None
            await self._store.pool_mgr.release(self._conn)
            self._conn = None

    # FIXME  def __del__(self):
