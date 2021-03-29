import iso8601
import datetime


class ISODatetimeFormatConverter:
    @staticmethod
    async def compare_iso_strings(assign_time_string, complete_time_string):
        """
        compares assignment and completion time
        :param assign_time_string:
        :param complete_time_string:
        :return: boolean
        """
        return assign_time_string < complete_time_string

    @staticmethod
    async def parse_iso_string(time_string):
        """
        parses string of iso datetime format
        :param time_string:
        :return: datetime
        """
        if time_string[-1] == "Z":
            time_string = time_string.replace("Z", "+00:00")
        return iso8601.parse_date(time_string).replace(tzinfo=None)

    @staticmethod
    async def parse_datetime(datetime_):
        """
        parses datetime to rfc datetime format
        :param datetime_:
        :return: time string
        """
        return datetime_.isoformat("T") + "Z"

    @staticmethod
    async def get_now():
        """
        :return: current UTC time
        """
        return datetime.datetime.utcnow()
