from .couriers_query import COURIERS_QUERY
from .orders_query import ORDERS_QUERY
from .available_orders_query import AVAILABLE_ORDERS_QUERY
from .available_orders import AvailableOrdersDefiner
from .couriers_orders_query import COURIERS_ORDERS_SEQUENCES_QUERY, \
    COURIERS_ORDERS_REGIONS_QUERY, COURIERS_ORDERS_LAST_COMPLETION_TIME
QUERY = (
    COURIERS_QUERY,
    ORDERS_QUERY,
    AVAILABLE_ORDERS_QUERY,
    AvailableOrdersDefiner,
    COURIERS_ORDERS_SEQUENCES_QUERY,
    COURIERS_ORDERS_REGIONS_QUERY,
    COURIERS_ORDERS_LAST_COMPLETION_TIME
)
