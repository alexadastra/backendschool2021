# as we don't need to address to carrying_capacity and earnings_coefficient
# in each handler (and at the same time), we can calculate values with functions
class CourierConfigurator:
    @staticmethod
    async def get_courier_carrying_capacity(courier_type):
        if courier_type == "car":
            return 50
        elif courier_type == "bike":
            return 15
        elif courier_type == "foot":
            return 10
        else:
            return 0

    @staticmethod
    async def get_courier_earnings_coefficient(courier_type):
        if courier_type == "car":
            return 9
        elif courier_type == "bike":
            return 5
        elif courier_type == "foot":
            return 2
        else:
            return 0
