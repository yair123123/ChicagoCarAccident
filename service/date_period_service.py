from repository.accidents_repository import *


def get_accidents_by_period(area_id, period, date):
    dict_func = {"month": get_accidents_by_month,
                 "week": get_accidents_by_week,
                 "day": get_accidents_by_day}
    return dict_func[period](area_id, date)
