import iso8601
import datetime


class ISODatetimeFormatConverter:
    @staticmethod
    async def compare_iso_strings(assign_time_string, complete_time_string):
        return assign_time_string < complete_time_string

    @staticmethod
    async def parse_iso_string(time_string):
        if time_string[-1] == "Z":
            time_string = time_string.replace("Z", "+00:00")
        return iso8601.parse_date(time_string).replace(tzinfo=None)

    @staticmethod
    async def parse_datetime(datetime_):
        return datetime_.isoformat("T") + "Z"

    @staticmethod
    async def get_now():
        return datetime.datetime.utcnow()
