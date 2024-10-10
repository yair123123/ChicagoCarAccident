from config.connect import *
from utils.csv_util import read_csv
from utils.data_utils import get_week_range


def init_chicago_accidents():
    monthly.drop()
    weekly.drop()
    daily.drop()
    accidents.drop()
    areas.drop()

    for row in read_csv("chicago_accidents.csv"):
        accident = {
            "date": row["CRASH_DATE"],
            'injuries': {
                "fatal": int(row['INJURIES_FATAL']) if row['INJURIES_FATAL'] else 0,
                "non_fatal": (
                        (int(row["INJURIES_INCAPACITATING"]) if row["INJURIES_INCAPACITATING"] else 0) +
                        (int(row["INJURIES_NON_INCAPACITATING"]) if row["INJURIES_NON_INCAPACITATING"] else 0)
                ),
                "total": int(row["INJURIES_TOTAL"]) if row["INJURIES_TOTAL"] else 0
            },
            'PRIM_CONTRIBUTORY_CAUSE': row['PRIM_CONTRIBUTORY_CAUSE']
        }

        accident_id = accidents.insert_one(accident).inserted_id

        areas_result = areas.update_one(
            {'area': row['BEAT_OF_OCCURRENCE']},
            {'$set': {"area": row["BEAT_OF_OCCURRENCE"]}, "$push": {"accidents": accident_id},
             "$inc": {"total_accident": 1}},
            upsert=True
        )

        week = get_week_range(row["CRASH_DATE"])
        weekly_dict = {
            'start': week[0],
            'end': week[1],
        }

        weekly_result = weekly.update_one(
            {'start': weekly_dict['start'], 'end': weekly_dict['end']},
            {'$set': weekly_dict, "$push": {"accidents": accident_id}, "$inc": {"total_accident": 1}},
            upsert=True
        )

        daily_dict = {
            'date': row['CRASH_DATE'],
        }

        daily_result = daily.update_one(
            {'date': row['CRASH_DATE']},
            {'$set': daily_dict, "$push": {"accidents": accident_id}, "$inc": {"total_accident": 1}},
            upsert=True
        )

        monthly_dict = {
            'year': row['CRASH_DATE'].split("/")[2].split(" ")[0],
            'month': row['CRASH_DATE'].split("/")[1],
        }

        monthly_result = monthly.update_one(
            {'year': monthly_dict['year'], 'month': monthly_dict['month']},
            {'$set': monthly_dict, "$push": {"accidents": accident_id}, "$inc": {"total_accident": 1}},
            upsert=True
        )
