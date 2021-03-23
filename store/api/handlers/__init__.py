from .some_view import SomeView
from .courier.couriers_imports import CouriersImportsView
from .order.orders_imports import OrdersImportsView

HANDLERS = (
    SomeView, CouriersImportsView, OrdersImportsView
)
