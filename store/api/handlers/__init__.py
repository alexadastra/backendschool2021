from .some_view import SomeView
from .courier.couriers_imports import CouriersImportsView
from .order.orders_imports import OrdersImportsView
from .courier.couriers import CourierView

HANDLERS = (
    SomeView, CouriersImportsView, CourierView, OrdersImportsView
)
