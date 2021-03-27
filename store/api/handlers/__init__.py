from .some_view import SomeView
from .courier.couriers_imports import CouriersImportsView
from .order.orders_imports import OrdersImportsView
from .courier.couriers_patch import CourierView
from .order.orders_assign import OrdersAssignmentView
from .order.orders_complete import OrdersCompletionView

HANDLERS = (
    CouriersImportsView, CourierView, OrdersImportsView, OrdersAssignmentView, OrdersCompletionView
)
