from marshmallow import ValidationError
import re


class TimeIntervalsConverter:
    @staticmethod
    def int_to_string(time):
        """
        :param time: number of minutes
        :return: hh:mm
        """
        hours = str(int(time / 60)) if time / 60 > 9 else "0" + str(int(time / 60))
        minutes = str(time % 60) if time % 60 > 9 else "0" + str(time % 60)
        return hours + ":" + minutes

    @staticmethod
    def int_to_string_interval(time_start, time_finish):
        """
        Converts time interval of ints to string
        :param time_start: number of minutes
        :param time_finish: number of minutes
        :return: hh:mm-hh:mm
        """
        return TimeIntervalsConverter.int_to_string(time_start) + "-" + \
               TimeIntervalsConverter.int_to_string(time_finish)

    @staticmethod
    def int_to_string_array(time_start_intervals, time_finish_intervals):
        """
        Converts time intervals of ints to string list
        :param time_start_intervals: list of number of minutes
        :param time_finish_intervals: list of number of minutes
        :return: ['hh:mm-hh:mm', 'hh:mm-hh:mm', ...]
        """
        if len(time_start_intervals) == 0:
            return []
        if len(time_start_intervals) == 1:
            return [TimeIntervalsConverter.int_to_string_interval(time_start_intervals[0], time_finish_intervals[0])]
        time_intervals = []
        time_start_intervals_unique = list(dict.fromkeys(time_start_intervals))
        time_finish_intervals_unique = list(dict.fromkeys(time_finish_intervals))
        for i in range(len(time_start_intervals_unique)):
            time_intervals \
                .append(TimeIntervalsConverter
                        .int_to_string_interval(time_start_intervals_unique[i], time_finish_intervals_unique[i]))
        return time_intervals

    @staticmethod
    def string_to_int(time):
        """
        :param time: hh:mm string
        :return: number of minutes
        """
        spl = time.split(":")
        hours, minutes = int(spl[0]), int(spl[1])
        return hours * 60 + minutes

    @staticmethod
    def string_to_int_interval(time_interval):
        """
        :param time_interval: hh:mm-hh:mm string
        :return: time_start int, time_finish int
        """
        spl = time_interval.split("-")
        return TimeIntervalsConverter.string_to_int(spl[0]), TimeIntervalsConverter.string_to_int(spl[1])

    @staticmethod
    def string_to_int_array(time_intervals):
        """
        :param time_intervals: ['hh:mm-hh:mm', 'hh:mm-hh:mm', ...]
        :return: time_start_intervals: list of number of minutes, time_finish_intervals: list of number of minutes
        """
        time_start_intervals = []
        time_finish_intervals = []
        for time_interval in time_intervals:
            time_start, time_finish = TimeIntervalsConverter.string_to_int_interval(time_interval)
            time_start_intervals.append(time_start)
            time_finish_intervals.append(time_finish)
        return time_start_intervals, time_finish_intervals

    @staticmethod
    def validate_time_mark(time_mark, value_title, i):
        """
        Validate hh:mm string
        :param time_mark:
        :param value_title:
        :param i:
        :return:
        """
        hours, minutes = int(time_mark.split(":")[0]), int(time_mark.split(":")[0])
        if hours > 23:
            raise ValidationError(
                'incorrect value for {} on index {}. {} is out of range'.format(value_title, i, hours)
            )
        if minutes > 59:
            raise ValidationError(
                'incorrect value for {} on index {}. {} is out of range'.format(value_title, i, minutes)
            )
        return hours, minutes

    @staticmethod
    def validate_hour_intervals_list(hour_intervals_list, value_title):
        """
        validates ['hh:mm-hh:mm', 'hh:mm-hh:mm', ...]
        :param hour_intervals_list:
        :param value_title:
        :return:
        """
        for i in range(len(hour_intervals_list)):
            hour_interval = hour_intervals_list[i]
            time_start, time_finish = hour_interval.split("-")
            # check if time interval isn't empty
            if time_start == time_finish:
                raise ValidationError(
                    'time interval {} is empty'.format(hour_interval)
                )
            # check if time mark is correct (hours are in [0..23], minutes are in [0..59])
            hours_start, minutes_start = TimeIntervalsConverter.validate_time_mark(time_start, value_title, i)
            hours_finish, minutes_finish = TimeIntervalsConverter.validate_time_mark(time_finish, value_title, i)
            # should we check "23:00"-"2:00" or "23:00"-"0:00"? yes indeed
            if hours_start * 60 + minutes_start > hours_finish * 60 + minutes_finish:
                raise ValidationError(
                    'time_start ({}) is greater than time_finish ({}).'.format(time_start, time_finish)
                )

    @staticmethod
    def validate_hour_intervals_with_regular_expressions(hour_intervals_list, value_title):
        """
        validates hh:mm string as regular expression
        :param hour_intervals_list:
        :param value_title:
        :return:
        """
        pattern = re.compile('^\d\d:\d\d-\d\d:\d\d$')
        for i in range(len(hour_intervals_list)):
            hour_interval = hour_intervals_list[i]
            if not pattern.match(hour_interval):
                raise ValidationError(
                    'incorrect format for {} on index {}.'.format(value_title, i)
                )

    @staticmethod
    def validate_hour_intervals(hour_intervals_list, value_title):
        TimeIntervalsConverter.validate_hour_intervals_with_regular_expressions(hour_intervals_list, value_title)
        TimeIntervalsConverter.validate_hour_intervals_list(hour_intervals_list, value_title)
