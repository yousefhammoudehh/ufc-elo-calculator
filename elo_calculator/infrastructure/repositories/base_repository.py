import json
from collections.abc import Callable
from typing import Any, cast
from uuid import UUID

from sqlalchemy import Table, and_, asc, case, delete, desc, func, insert, literal_column, select, update
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.functions import Function
from sqlalchemy.sql.selectable import Select
from sqlalchemy.types import Boolean, Date, DateTime, Float, Integer, String

from elo_calculator.configs.env import REDIS_TTL
from elo_calculator.configs.log import get_logger
from elo_calculator.domain.entities.base_entity import BaseEntityBase
from elo_calculator.domain.shared.enumerations import StrEnum
from elo_calculator.errors import DatabaseException
from elo_calculator.infrastructure.external_services.caching import CacheManager, CachePrefix
from elo_calculator.utils.converters import hash_dict, hash_str
from elo_calculator.utils.date_parser import iso_str_to_datetime, str_to_date

logger = get_logger()

supported_operators = ['=', '!=', '>', '>=', '<', '<=', 'like', 'ilike', 'in', 'not_in']

# Supported operators mapping
OPERATORS_MAPPING: dict[str, Callable[..., Any]] = {
    '=': operators.eq,
    '!=': operators.ne,
    '>': operators.gt,
    '>=': operators.ge,
    '<': operators.lt,
    '<=': operators.le,
    'like': operators.like_op,
    'ilike': operators.ilike_op,
    'in': lambda col, val: col.in_(val),
    'not_in': lambda col, val: ~col.in_(val),
}


class CacheMethods:
    GET = 'get'
    ALL = 'all'
    PAGINATED = 'paginated'


class BaseRepository[T: BaseEntityBase]:
    def __init__(
        self,
        connection: AsyncConnection,
        model_cls: type[T],
        table: Table,
        cache_prefix: CachePrefix | str | None = None,
        cache_ttl: int | None = None,
    ):
        self.connection = connection
        self.table = table
        self.model_cls = model_cls
        self.cache_prefix = (
            cache_prefix.value if isinstance(cache_prefix, CachePrefix) else (cache_prefix or table.name)
        )
        self.cache = CacheManager(cache_ttl or REDIS_TTL)

    async def add(self, entity: T, include_id: bool = False) -> T:
        excluded_fields = list(entity.config.db_excluded_fields)
        if not include_id:
            excluded_fields.append('id')
            excluded_fields.append('created_at')
        data = entity.to_dict(excluded_fields, False)
        cmd = insert(self.table).values(**data).returning(*self.table.columns)
        result = await self.connection.execute(cmd)
        row = result.first()
        if not row:
            raise DatabaseException('Filed to create row', self.model_cls.__name__, self.table.name)

        await self.cache.delete_matching_prefix(self._get_cache_key())
        return self._map_row_to_model(row._asdict())

    async def get_by_id(self, entity_id: UUID) -> T | None:
        cache_key = self._get_cache_key(f'{CacheMethods.GET}:{entity_id}')

        if cached_data := await self.cache.get_json(cache_key):
            return self._map_row_to_model(cast(dict[str, Any], cached_data))

        cmd = self._get_select_statement().where(self.table.c.id == entity_id)
        result = await self.connection.execute(cmd)
        row = result.first()
        if not row:
            return None

        data = row._asdict()

        await self.cache.set_json(cache_key, data)
        return self._map_row_to_model(data)

    async def get_all(
        self, filters: dict[str, Any] | None = None, sort_by: str = 'created_at', order: str = 'desc'
    ) -> list[T]:
        cache_vars = f'filters:{hash_dict(filters)}:sort_by:{sort_by}:order:{order}'
        cache_key = self._get_cache_key(f'{CacheMethods.ALL}:{hash_str(cache_vars)}')

        if cached_data := await self.cache.get_json(cache_key):
            return [self._map_row_to_model(row) for row in cast(list[dict[str, Any]], cached_data)]

        cmd = self._get_select_with_filters(filters, sort_by, order)
        result = await self.connection.execute(cmd)

        rows = [row._asdict() for row in result.all()]

        await self.cache.set_json(cache_key, rows)
        return [self._map_row_to_model(row) for row in rows]

    async def get_paginated(
        self, page: int, limit: int, sort_by: str = 'created_at', order: str = 'desc'
    ) -> tuple[list[T], int]:
        return await self.get_paginated_with_filters(page, limit, None, sort_by, order)

    async def get_paginated_with_filters(
        self,
        page: int,
        limit: int,
        filters: dict[str, Any] | None = None,
        sort_by: str = 'created_at',
        order: str = 'desc',
    ) -> tuple[list[T], int]:
        cache_vars = f'page:{page}:limit:{limit}:filters:{hash_dict(filters)}' + f':sort_by:{sort_by}:order:{order}'

        cache_key = self._get_cache_key(f'{CacheMethods.PAGINATED}: {hash_str(cache_vars)}')
        if cached_data := await self.cache.get_json(cache_key):
            cached_data = cast(dict[str, Any], cached_data)
            return [self._map_row_to_model(row) for row in cached_data['rows']], cached_data['total']

        cmd = self._get_select_with_filters(filters, sort_by, order)

        # Query to get the total count
        count_cmd = cmd.with_only_columns(func.count(self.table.c.id).label('total')).order_by(None)

        total_result = await self.connection.execute(count_cmd)
        total = total_result.scalar_one()

        # Query to get paginated data
        paginated_cmd = cmd.offset((page - 1) * limit).limit(limit)
        result = await self.connection.execute(paginated_cmd)

        rows = [row._asdict() for row in result.all()]
        await self.cache.set_json(cache_key, {'rows': rows, 'total': total})
        return [self._map_row_to_model(row) for row in rows], total

    async def update(self, entity_id: UUID, data: dict[str, Any]) -> T:
        invalid_keys = [k for k in data if k not in self.table.c and k not in self.model_cls.config.db_excluded_fields]
        if invalid_keys:
            raise DatabaseException(
                f'Invalid column(s) in data: {", ".join(invalid_keys)}', self.model_cls.__name__, self.table.name
            )

        update_data = {k: v for k, v in data.items() if k not in self.model_cls.config.db_excluded_fields}
        cmd = (
            update(self.table).where(self.table.c.id == entity_id).values(**update_data).returning(*self.table.columns)
        )
        result = await self.connection.execute(cmd)
        row = result.first()
        if not row:
            raise DatabaseException('Failed to update row', self.model_cls.__name__, self.table.name)

        await self.cache.flush_db()
        return self._map_row_to_model(row._asdict())

    async def delete(self, entity_id: UUID) -> T:
        cmd = delete(self.table).where(self.table.c.id == entity_id).returning(*self.table.columns)
        result = await self.connection.execute(cmd)
        row = result.first()
        if not row:
            raise DatabaseException('Failed to delete row', self.model_cls.__name__, self.table.name)

        await self.cache.flush_db()
        return self._map_row_to_model(row._asdict())

    async def bulk_insert(self, entities: list[T]) -> list[T]:
        if not entities:
            raise DatabaseException('there is no data to insert', self.model_cls.__name__, self.table.name)

        data = [
            {**entity.to_dict([*entity.config.db_excluded_fields, 'id', 'created_at'], False)} for entity in entities
        ]
        stmt = self.table.insert().returning(*self.table.columns)
        result = await self.connection.execute(stmt, data)

        await self.cache.delete_matching_prefix(self._get_cache_key())
        return [self._map_row_to_model(row._asdict()) for row in result.all()]

    async def bulk_update(self, updates: list[dict[str, Any]]) -> list[T]:
        if not updates:
            raise DatabaseException('No data to update', self.model_cls.__name__, self.table.name)

        for data in updates:
            invalid_keys = [
                k for k in data if k not in self.table.c and k not in self.model_cls.config.db_excluded_fields
            ]
            if invalid_keys:
                raise DatabaseException(
                    f'Invalid column(s) in update data: {", ".join(invalid_keys)}',
                    self.model_cls.__name__,
                    self.table.name,
                )

        keys = [key for key in updates[0] if key != 'id' and key not in self.model_cls.config.db_excluded_fields]
        case_statements = {
            col: case(
                *[(self.table.c.id == update['id'], update[col]) for update in updates if col in update],
                else_=getattr(self.table.c, col),
            )
            for col in keys
        }

        stmt = (
            update(self.table)
            .where(self.table.c.id.in_([update['id'] for update in updates]))
            .values(case_statements)
            .returning(*self.table.columns)
        )
        result = await self.connection.execute(stmt)

        await self.cache.flush_db()
        return [self._map_row_to_model(row._asdict()) for row in result.all()]

    async def bulk_delete(self, ids: list[UUID]) -> list[T]:
        if not ids:
            raise DatabaseException('there is no data to delete', self.model_cls.__name__, self.table.name)

        stmt = delete(self.table).where(self.table.c.id.in_(ids)).returning(*self.table.columns)
        result = await self.connection.execute(stmt)

        await self.cache.flush_db()
        return [self._map_row_to_model(row._asdict()) for row in result.all()]

    def _get_select_statement(self) -> Select[tuple[Any]]:
        return select(self.table)

    def _get_select_with_filters(
        self, filters: dict[str, Any] | None = None, sort_by: str = 'created_at', order: str = 'desc'
    ) -> Select[tuple[Any]]:
        cmd = self._get_select_statement()

        expressions = self._parse_filters(filters)
        if expressions is not None:
            cmd = cmd.where(expressions)

        if sort_by not in self.table.c:
            return cmd

        return cmd.order_by(asc(self.table.c[sort_by]) if order == 'asc' else desc(self.table.c[sort_by]))

    def _parse_filters(self, filters: dict[str, Any] | None = None) -> ColumnElement[bool] | None:  # noqa: PLR0912
        if not filters:
            return None
        expressions: list[Any] = []

        for key, value in filters.items():
            column_name, operator = key.split(':') if ':' in key else (key, '=')

            if operator not in OPERATORS_MAPPING or column_name not in self.table.c:
                continue
            column = self.table.c[column_name]
            is_list_op = operator in ['in', 'not_in']

            try:
                parsed_value = value
                if isinstance(column.type, Integer):
                    if is_list_op:
                        parsed_value = [int(v) for v in value.split(',')] if isinstance(value, str) else value
                    else:
                        parsed_value = int(value)
                elif isinstance(column.type, Float):
                    if is_list_op:
                        parsed_value = [float(v) for v in value.split(',')] if isinstance(value, str) else value
                    else:
                        parsed_value = float(value)
                elif isinstance(column.type, Boolean):
                    parsed_value = value.lower() in ['true', '1'] if isinstance(value, str) else bool(value)
                elif isinstance(column.type, DateTime):
                    parsed_value = iso_str_to_datetime(value)
                    if not parsed_value:
                        continue
                elif isinstance(column.type, Date):
                    parsed_value = str_to_date(value)
                    if not parsed_value:
                        continue
                elif isinstance(column.type, String) and is_list_op:
                    parsed_value = value.split(',') if isinstance(value, str) else value
                elif isinstance(column.type, StrEnum) and is_list_op:
                    parsed_value = json.loads(''.join(value)) if isinstance(value, str) else value
                expressions.append(OPERATORS_MAPPING[operator](column, parsed_value))
            except Exception as e:
                logger.warning(f'failed to parse filter with key: {key}, value: {value}. Error: {e}')
                continue

        return and_(*expressions) if expressions else None

    def _map_row_to_model(self, row: dict[str, Any]) -> T:
        return self.model_cls.from_dict(row)

    def _build_json_object(self, table: Table) -> Function[Any]:
        args = []
        for col in table.columns:
            args.extend([literal_column(f"'{col.name}'"), col])
        return func.json_build_object(*args)

    def _get_cache_key(self, method: str | None = None) -> str:
        return f'{self.cache_prefix}:{method}' if method else self.cache_prefix
