from enum import Enum, unique

from sqlalchemy import (
    Column, Date, Enum as PgEnum, ForeignKey, ForeignKeyConstraint, Integer,
    MetaData, String, Table, Float, DateTime
)

# SQLAlchemy рекомендует использовать единый формат для генерации названий для
# индексов и внешних ключей.
# https://docs.sqlalchemy.org/en/13/core/constraints.html#configuring-constraint-naming-conventions
convention = {
    'all_column_names': lambda constraint, table: '_'.join([
        column.name for column in constraint.columns.values()
    ]),
    'ix': 'ix__%(table_name)s__%(all_column_names)s',
    'uq': 'uq__%(table_name)s__%(all_column_names)s',
    'ck': 'ck__%(table_name)s__%(constraint_name)s',
    'fk': 'fk__%(table_name)s__%(all_column_names)s__%(referred_table_name)s',
    'pk': 'pk__%(table_name)s'
}

metadata = MetaData(naming_convention=convention)


@unique
class CourierType(Enum):
    foot = 'foot'
    bike = 'bike'
    car = 'car'


couriers_imports_table = Table(
    'couriers_imports',
    metadata,
    Column('import_id', Integer, primary_key=True)
)

couriers_table = Table(
    'couriers',
    metadata,
    Column('courier_id', Integer, primary_key=True),
    Column('import_id', Integer,
           ForeignKey('couriers_imports.import_id'), primary_key=True),
    Column('type', PgEnum(CourierType, name='type'), nullable=False)
)

regions_table = Table(
    'regions',
    metadata,
    Column('region_id', Integer, primary_key=True),
    Column('region_numeric_value', Integer, nullable=False)
)

couriers_regions_table = Table(
    'couriers_regions',
    metadata,
    Column('import_id', Integer, primary_key=True),
    Column('courier_id', Integer, primary_key=True),
    Column('region_id', Integer,
           ForeignKey('regions.region_id'), primary_key=True),
    ForeignKeyConstraint(
        ('import_id', 'courier_id'),
        ('couriers.import_id', 'couriers.courier_id')
    )
)

working_hours_table = Table(
    'working_hours',
    metadata,
    Column('working_hours_id', primary_key=True),
    Column('time_start', Integer, nullable=False),
    Column('time_finish', Integer, nullable=False)
)

couriers_working_hours_table = Table(
    'couriers_working_hours',
    metadata,
    Column('import_id', Integer, primary_key=True),
    Column('courier_id', Integer, primary_key=True),
    Column('working_hours_id', Integer,
           ForeignKey('working_hours.working_hours_id'), primary_key=True)
)

orders_imports_table = Table(
    'orders_imports',
    metadata,
    Column('import_id', Integer, primary_key=True)
)

orders_table = Table(
    'orders',
    metadata,
    Column('order_id', Integer, primary_key=True),
    Column('import_id', Integer,
           ForeignKey('orders_imports.import_id'), primary_key=True),
    Column('weight', Float, nullable=False),
    Column('region', Integer, nullable=False),
    Column('assignment_time', DateTime, default=None),
    Column('completion_time', DateTime, default=None)
)

delivery_hours_table = Table(
    'delivery_hours',
    metadata,
    Column('delivery_hours_id', primary_key=True),
    Column('time_start', Integer, nullable=False),
    Column('time_finish', Integer, nullable=False)
)

orders_delivery_hours_table = Table(
    'orders_delivery_hours',
    metadata,
    Column('import_id', Integer, primary_key=True),
    Column('courier_id', Integer, primary_key=True),
    Column('delivery_hours_id', Integer,
           ForeignKey('delivery_hours.delivery_hours_id'), primary_key=True)
)
