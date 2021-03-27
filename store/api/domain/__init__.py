from .time_intarvals_converter import TimeIntervalsConverter
from .couriers_orders_resolver import CouriersOrdersResolver
from .courier_configurator import CourierConfigurator
from .iso_datetime_formats_converter import ISODatetimeFormatConverter

DOMAIN = (
    TimeIntervalsConverter, CouriersOrdersResolver, CourierConfigurator, ISODatetimeFormatConverter
)
