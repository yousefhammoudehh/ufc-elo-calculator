from abc import ABC
from typing import Any, Callable, Generic, List, Optional, Type, TypeVar
from uuid import UUID

from sqlalchemy import Table, and_, asc, delete, desc, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.selectable import Select
from sqlalchemy.types import Boolean, Date, DateTime, Float, Integer, String

from elo_calculator.domain.entities.base_entity import BaseEntity
from elo_calculator.errors import DatabaseError
from elo_calculator.utils.date_parser import parse_date, parse_iso_date

supported_operators = ["=", "!=", ">", ">=", "<", "<=", "like", "ilike", "in", "not_in"]

# Supported operators mapping
OPERATORS_MAPPING: dict[str, Callable[..., Any]] = {
    "=": operators.eq,
    "!=": operators.ne,
    ">": operators.gt,
    ">=": operators.ge,
    "<": operators.lt,
    "<=": operators.le,
    "like": operators.like_op,
    "ilike": operators.ilike_op,
    "in": lambda col, val: col.in_(val),
    "not_in": lambda col, val: ~col.in_(val),
}


T = TypeVar('T', bound=BaseEntity)


class BaseRepository(ABC, Generic[T]):
    def __init__(self, connection: AsyncConnection, model_cls: Type[T], table: Table):
        self.connection = connection
        self.table = table
        self.model_cls = model_cls

    async def add(self, entity: T) -> T:
        data = entity.to_dict()
        del data['id']
        del data['created_at']
        cmd = insert(self.table).values(**data).returning(*self.table.columns)
        result = await self.connection.execute(cmd)
        row = result.first()
        if not row:
            raise DatabaseError('Filed to create row',
                                {'model_cls': self.model_cls.__name__, 'table': self.table.name})
        return self.model_cls.from_dict(dict(row._mapping))

    async def get_by_id(self, id: UUID) -> Optional[T]:
        cmd = select(self.table).where(self.table.c.id == id)
        result = await self.connection.execute(cmd)
        row = result.first()
        return self.model_cls.from_dict(dict(row._mapping)) if row else None

    async def get_all(self, filters: Optional[dict[str, Any]] = None,
                      sort_by: str = 'created_at', order: str = 'desc') -> List[T]:
        cmd = self._get_select_with_filters(filters, sort_by, order)
        result = await self.connection.execute(cmd)
        return [self.model_cls.from_dict(dict(row._mapping)) for row in result.all()]

    async def get_paginated(self, page: int, limit: int,
                            sort_by: str = 'created_at', order: str = 'desc') -> tuple[List[T], int]:
        return await self.get_paginated_with_filters(page, limit, None, sort_by, order)

    async def get_paginated_with_filters(self, page: int, limit: int, filters: Optional[dict[str, Any]] = None,
                                         sort_by: str = 'created_at', order: str = 'desc') -> tuple[List[T], int]:

        cmd = self._get_select_with_filters(filters, sort_by, order)

        # Query to get the total count
        count_cmd = cmd.with_only_columns(func.count(self.table.c.id).label('total')).order_by(None)

        total_result = await self.connection.execute(count_cmd)
        total = total_result.scalar_one()

        # Query to get paginated data
        paginated_cmd = cmd.offset((page - 1) * limit).limit(limit)
        rows = await self.connection.execute(paginated_cmd)

        return [self.model_cls.from_dict(dict(row._mapping)) for row in rows.all()], total

    async def update(self, id: UUID, data: dict[str, Any]) -> T:
        cmd = update(self.table).where(self.table.c.id == id).values(
            **data).returning(*self.table.columns)
        result = await self.connection.execute(cmd)
        row = result.first()
        if not row:
            raise DatabaseError('Filed to update row',
                                {'model_cls': self.model_cls.__name__, 'table': self.table.name, 'id': str(id)})
        return self.model_cls.from_dict(dict(row._mapping))

    async def delete(self, id: UUID) -> T:
        # TODO: implement soft delete
        cmd = delete(self.table).where(self.table.c.id == id).returning(*self.table.columns)
        result = await self.connection.execute(cmd)
        row = result.first()
        if not row:
            raise DatabaseError('Filed to delete row',
                                {'model_cls': self.model_cls.__name__, 'table': self.table.name, 'id': str(id)})
        return self.model_cls.from_dict(dict(row._mapping))

    def _get_select_with_filters(self, filters: Optional[dict[str, Any]] = None,
                                 sort_by: str = 'created_at', order: str = 'desc') -> Select[tuple[Any]]:
        cmd = select(self.table)

        expressions = self._parse_filters(filters)
        if expressions is not None:
            cmd = cmd.where(expressions)

        if sort_by not in self.table.c:
            return cmd

        return cmd.order_by(asc(self.table.c[sort_by])if order == 'asc' else desc(self.table.c[sort_by]))

    def _parse_filters(self, filters: Optional[dict[str, Any]]) -> Optional[ColumnElement[bool]]:
        if not filters:
            return None
        expressions: List[Any] = []

        for key, value in filters.items():
            column_name, operator = key.split(':') if ':' in key else (key, '=')

            if operator not in OPERATORS_MAPPING or column_name not in self.table.c:
                continue
            column = self.table.c[column_name]
            is_list_op = operator in ['in', 'not_in']

            try:
                if isinstance(column.type, Integer):
                    if is_list_op:
                        value = [int(v) for v in value.split(',')] if isinstance(value, str) else value
                    else:
                        value = int(value)
                elif isinstance(column.type, Float):
                    if is_list_op:
                        value = [float(v) for v in value.split(',')] if isinstance(value, str) else value
                    else:
                        value = float(value)
                elif isinstance(column.type, Boolean):
                    value = value.lower() in ['true', '1'] if isinstance(value, str)else bool(value)
                elif isinstance(column.type, DateTime):
                    value = parse_iso_date(value)
                    if not value:
                        continue
                elif isinstance(column.type, Date, ):
                    value = parse_date(value)
                    if not value:
                        continue
                elif isinstance(column.type, String):
                    if is_list_op:
                        value = value.split(',') if isinstance(value, str) else value
                expressions.append(OPERATORS_MAPPING[operator](column, value))
            except Exception:
                continue

        return and_(*expressions) if expressions else None
