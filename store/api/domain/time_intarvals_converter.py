

class TimeIntervalsConverter:
    @staticmethod
    def int_to_string(time):
        hours = str(int(time / 60))
        minutes = str(time % 60) if time % 60 > 9 else "0" + str(time % 60)
        return hours + ":" + minutes

    @staticmethod
    def int_to_string_interval(time_start, time_finish):
        return TimeIntervalsConverter.int_to_string(time_start) + "-" + \
               TimeIntervalsConverter.int_to_string(time_finish)

    @staticmethod
    def int_to_string_array(time_start_intervals, time_finish_intervals):
        if len(time_start_intervals) == 0:
            return []
        if len(time_start_intervals) == 1:
            return [TimeIntervalsConverter.int_to_string_interval(time_start_intervals[0], time_finish_intervals[0])]
        time_intervals = []
        time_start_intervals_unique = list(dict.fromkeys(time_start_intervals))
        time_finish_intervals_unique = list(dict.fromkeys(time_finish_intervals))
        for i in range(len(time_start_intervals_unique)):
            time_intervals\
                .append(TimeIntervalsConverter
                        .int_to_string_interval(time_start_intervals_unique[i], time_finish_intervals_unique[i]))
        return time_intervals
