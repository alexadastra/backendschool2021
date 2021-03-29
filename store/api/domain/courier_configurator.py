# as we don't need to address to carrying_capacity and earnings_coefficient
# in each handler (and at the same time), we can calculate values with functions
class CourierConfigurator:
    @staticmethod
    async def get_courier_carrying_capacity(courier_type):
        """
        Calculates carrying capacity for couriers
        :param courier_type:
        :return:
        """
        if courier_type == "car":
            return 50
        elif courier_type == "bike":
            return 15
        elif courier_type == "foot":
            return 10
        else:
            return -1

    @staticmethod
    async def get_courier_earnings_coefficient(courier_type):
        """
        Calculates earnings coefficient for couriers
        :param courier_type:
        :return:
        """
        if courier_type == "car":
            return 9
        elif courier_type == "bike":
            return 5
        elif courier_type == "foot":
            return 2
        else:
            return -1

    @staticmethod
    async def calculate_earnings(count, courier_type):
        """
        Calculates earnings for couriers
        :param count: how much order sequences courier delivered
        :param courier_type:
        :return:
        """
        return count * 500 * await CourierConfigurator.get_courier_earnings_coefficient(courier_type)

    @staticmethod
    async def calculate_rating(t):
        """
        Calculate rating for courier (5 if he snipers the order in 0s, 0 if he hardly fit in hour)
        :param t: minimal average delivery time in couriers regions
        :return:
        """
        return round((60 * 60 - min(round(t), 60*60)) / (60*60) * 5, 2)
