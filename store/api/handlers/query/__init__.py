from .couriers_query import COURIERS_QUERY
from .orders_query import ORDERS_QUERY
from .available_orders_query import AVAILABLE_ORDERS_QUERY
from .available_orders import AvailableOrdersDefiner

QUERY = (
    COURIERS_QUERY, ORDERS_QUERY, AVAILABLE_ORDERS_QUERY, AvailableOrdersDefiner
)
