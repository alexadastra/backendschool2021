from marshmallow import Schema, ValidationError, validates, validates_schema
from marshmallow.fields import Float, Int, List, Nested, Str
from marshmallow.validate import Length, OneOf, Range

from store.db.schema import CourierType
from store.api.domain import TimeIntervalsConverter


class CouriersNested(Nested):
    def _deserialize(self, *args, **kwargs):
        try:
            return super()._deserialize(*args, **kwargs)
        except ValidationError as err:
            err.messages = {'validation_error': {'couriers': [{'id': i} for i in err.messages]}}
            raise err


class OrdersNested(Nested):
    def _deserialize(self, *args, **kwargs):
        try:
            return super()._deserialize(*args, **kwargs)
        except ValidationError as err:
            err.messages = {'validation_error': {'orders': [{'id': i} for i in err.messages]}}
            raise err


class CourierItemSchema(Schema):
    courier_id = Int(validate=Range(min=0), strict=True, required=True)
    courier_type = Str(validate=OneOf([courier_type.value for courier_type in CourierType]), required=True)
    regions = List(Int(validate=Range(min=0)), strict=True, required=True)
    working_hours = List(Str(), required=True)

    @validates('working_hours')
    def validate_working_hours(self, value):
        TimeIntervalsConverter.validate_hour_intervals(value, "working hours")


class CouriersPostRequestSchema(Schema):
    data = CouriersNested(CourierItemSchema, many=True, required=True,
                          validate=Length(max=10000))

    @validates_schema
    def validate_unique_courier_id(self, data, **_):
        courier_ids_set = set()
        couriers_ids_list = list()
        for courier in data['data']:
            courier_ids_set.add(courier['courier_id'])
            couriers_ids_list.append(courier['courier_id'])
        if len(couriers_ids_list) != len(courier_ids_set):
            raise ValidationError(
                {'data': {'validation_error': [
                    {'id': i
                     } for i in [item for item in courier_ids_set if couriers_ids_list.count(item) > 1]
                ]}})


class SingleIdSchema(Schema):
    id = Int(validate=Range(min=0), strict=True, required=True)


class CouriersIdsSchema(Schema):
    couriers = Nested(SingleIdSchema, many=True, required=True,
                      validate=Length(max=10000))


class CourierGetResponseSchema(CourierItemSchema):
    rating = Float(validate=Range(min=0), strict=True)
    earnings = Int(validate=Range(min=0), strict=True)


class CourierUpdateRequestSchema(Schema):
    type = Str(validate=OneOf([courier_type.value for courier_type in CourierType]))
    regions = List(Int(validate=Range(min=0)), strict=True)
    working_hours = List(Str())

    @validates('working_hours')
    def validate_working_hours(self, value):
        TimeIntervalsConverter.validate_hour_intervals(value, "working hours")


class OrderItemSchema(Schema):
    order_id = Int(validate=Range(min=0), strict=True, required=True)
    weight = Float(validate=Range(min=0.01, max=50), strict=True, required=True)
    region = Int(validate=Range(min=0), strict=True, required=True)
    delivery_hours = List(Str(validate=Length(min=1, max=10000)), strict=True, required=True)

    @validates('delivery_hours')
    def validate_delivery_hours(self, value):
        if len(value) == 0:
            raise ValidationError("delivery hours must not be empty!")
        TimeIntervalsConverter.validate_hour_intervals(value, "delivery hours")


class OrdersPostRequestSchema(Schema):
    data = OrdersNested(OrderItemSchema, many=True, required=True,
                        validate=Length(max=10000))

    @validates_schema
    def validate_unique_courier_id(self, data, **_):
        ids_set = set()
        ids_list = list()
        for order in data['data']:
            ids_set.add(order['order_id'])
            ids_list.append(order['order_id'])
        if len(ids_list) != len(ids_set):
            raise ValidationError(
                {'data': {'validation_error': [
                    {'id': i
                     } for i in [item for item in ids_set if ids_list.count(item) > 1]
                ]}})


class OrdersIdsSchema(Schema):
    orders = Nested(SingleIdSchema, many=True, required=True,
                    validate=Length(max=10000))


class OrdersGetResponseSchema(OrderItemSchema):
    courier_id = Int()
    assign_time = Str(strict=True)
    delivery_start_time = Str(strict=True)
    complete_time = Str(strict=True)


class OrdersAssignPostResponseSchema(Schema):
    # courier can't carry more than maximum amount of orders with minimal weight
    orders = Nested(SingleIdSchema, many=True, required=True,
                    validate=Length(min=0, max=5000))
    assign_time = Str()


class OrdersAssignPostRequestSchema(Schema):
    courier_id = Int(validate=Range(min=0), strict=True, required=True)


class OrdersCompletePostRequestSchema(Schema):
    courier_id = Int(validate=Range(min=0), strict=True, required=True)
    order_id = Int(validate=Range(min=0), strict=True, required=True)
    complete_time = Str(required=True)


class OrdersCompletePostResponseSchema(Schema):
    order_id = Int(validate=Range(min=0), strict=True, required=True)
