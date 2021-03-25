

class TimeIntervalsConverter:
    @staticmethod
    def int_to_string(time):
        hours = str(int(time / 60)) if time / 60 > 9 else "0" + str(int(time / 60))
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

    @staticmethod
    def string_to_int(time):
        spl = time.split(":")
        hours, minutes = int(spl[0]) , int(spl[1])
        return hours * 60 + minutes

    @staticmethod
    def string_to_int_interval(time_interval):
        spl = time_interval.split("-")
        return TimeIntervalsConverter.string_to_int(spl[0]), TimeIntervalsConverter.string_to_int(spl[1])

    @staticmethod
    def string_to_int_array(time_intervals):
        time_start_intervals = []
        time_finish_intervals = []
        for time_interval in time_intervals:
            time_start, time_finish = TimeIntervalsConverter.string_to_int_interval(time_interval)
            time_start_intervals.append(time_start)
            time_finish_intervals.append(time_finish)
        return time_start_intervals, time_finish_intervals
