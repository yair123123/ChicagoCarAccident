from datetime import datetime, timedelta

def get_week_range(date_str):
    only_date = date_str.split(" ")[0]
    date_obj = datetime.strptime(only_date, '%m/%d/%Y')
    start_of_week = date_obj - timedelta(days=date_obj.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    return start_of_week, end_of_week
