from .courier.couriers_imports import CouriersImportsView
from .order.orders_imports import OrdersImportsView
from .courier.couriers import CouriersView
from .order.orders_assign import OrdersAssignmentView
from .order.orders_complete import OrdersCompletionView

HANDLERS = (
    CouriersImportsView,  # POST /couriers
    CouriersView,  # PATCH /couriers/{id}
    OrdersImportsView,  # POST /orders
    OrdersAssignmentView,  # POST /orders/assign
    OrdersCompletionView,   # POST /orders/complete
)
