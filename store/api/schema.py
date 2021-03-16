from datetime import date

from marshmallow import Schema, ValidationError, validates, validates_schema
from marshmallow.fields import Date, Dict, Float, Int, List, Nested, Str
from marshmallow.validate import Length, OneOf, Range

from store.db.schema import CourierType
import re


def validate_hour_intervals_list(hour_intervals_list, value_title):
    for i in range(len(hour_intervals_list)):
        hour_interval = hour_intervals_list[i]
        for time_mark in hour_interval.split("-"):
            hours, minutes = time_mark.split(":")[0], time_mark.split(":")[0]
            if hours > 23:
                raise ValidationError(
                    'incorrect value for {} on index {}. {} is out of range'.format(value_title, i, hours)
                )
            if minutes > 59:
                raise ValidationError(
                    'incorrect value for {} on index {}. {} is out of range'.format(value_title, i, minutes)
                )


def validate_hour_intervals_with_regular_expressions(hour_intervals_list, value_title):
    pattern = re.compile('^\d\d:\d\d-\d\d:\d\d$')
    for i in range(len(hour_intervals_list)):
        hour_interval = hour_intervals_list[i]
        if not pattern.match(hour_interval):
            raise ValidationError(
                'incorrect format for {} on index {}.'.format(value_title, i)
            )


class CourierItemSchema(Schema):
    courier_id = Int(validate=Range(min=0), strict=True, required=True)
    courier_type = Str(validate=OneOf([courier_type.value for courier_type in CourierType]), required=True)
    regions = List(Int(validate=Range(min=0)), strict=True, required=True)
    working_hours = List(Str(), required=True)

    @validates('working_hours')
    def validate_working_hours(self, value):
        validate_hour_intervals_with_regular_expressions(value, "working hours")
        validate_hour_intervals_list(value, "working_hours")


class CouriersPostRequestSchema(Schema):
    data = Nested(CourierItemSchema, many=True, required=True,
                  validate=Length(max=10000))  # rewrite according to courier schema

    @validates_schema
    def validate_unique_courier_id(self, data, **_):
        courier_ids = set()
        for courier in data['courier_id']:
            if courier['courier_id'] in courier_ids:
                raise ValidationError(
                    'courier_id %r is not unique' % courier['courier_id']
                )
            courier_ids.add(courier['citizen_id'])


class SingleIdSchema(Schema):
    id = Int(validate=Range(min=0), strict=True, required=True)


class CouriersIdsSchema(Schema):
    couriers = Nested(SingleIdSchema, many=True, required=True,
                      validate=Length(max=10000))  # rewrite according to response schema


class CourierGetResponseSchema(CourierItemSchema):
    rating = Float(validate=Range(min=0), strict=True)
    earnings = Int(validate=Range(min=0), strict=True, required=True)


class CourierUpdateRequest(Schema):
    courier_type = Str(validate=OneOf([courier_type.value for courier_type in CourierType]))
    regions = List(Int(validate=Range(min=0)), strict=True)
    working_hours = List(Str())

    @validates('working_hours')
    def validate_working_hours(self, value):
        validate_hour_intervals_with_regular_expressions(value, "working hours")
        validate_hour_intervals_list(value, "working hours")


class OrderItemSchema(Schema):
    order_id = Int(validate=Range(min=0), strict=True, required=True)
    weight = Float(validate=Range(min=0), strict=True, required=True)
    region = Int(validate=Range(min=0), strict=True, required=True)
    delivery_hours = List(Str(), required=True)

    @validates('delivery_hours')
    def validate_working_hours(self, value):
        validate_hour_intervals_with_regular_expressions(value, "delivery hours")
        validate_hour_intervals_list(value, "delivery hours")


class OrdersPostRequest(Schema):
    data = Nested(OrderItemSchema, many=True, required=True,
                  validate=Length(max=10000))  # rewrite according to courier schema


class OrdersIds(Schema):
    items = Nested(SingleIdSchema, many=True, required=True,
                   validate=Length(max=10000))  # rewrite according to response schema


class AssignTime(Schema):
    assign_time = Str()


class OrdersAssignPostRequest(Schema):
    courier_id = Int(validate=Range(min=0), strict=True, required=True)


class OrdersCompletePostRequest(Schema):
    courier_id = Int(validate=Range(min=0), strict=True, required=True)
    order_id = Int(validate=Range(min=0), strict=True, required=True)
    complete_time = Str(required=True)


class OrdersCompletePostResponse(Schema):
    order_id = Int(validate=Range(min=0), strict=True, required=True)
