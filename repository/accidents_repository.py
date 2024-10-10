from datetime import datetime

from pymongo.errors import PyMongoError
from returns.result import Success, Failure

from config.connect import daily, weekly, monthly, areas, accidents


def indexing_collection():
    try:
        daily.create_index({"date": 1})
        weekly.create_index({"start": 1})
        monthly.create_index({"year": 1, "month": 1})
        areas.create_index({"area": 1})
        return Success("indexing success")
    except PyMongoError as e:
        return Failure(str(e))


def get_accident_by_area(id_area: id):
    try:

        return Success(list(areas.aggregate([
            {'$match': {"area": id_area}},
            {
                '$lookup': {
                    'from': 'accidents',
                    'localField': 'accidents',
                    'foreignField': '_id',
                    'as': 'accidents'
                }
            }
        ])))
    except PyMongoError as e:
        return Failure(str(e))


def get_accidents_by_week(id_area: id, date_start: list[int]):
    breakpoint()
    date_start = datetime(year=int(date_start[2]), month=int(date_start[1]), day=int(date_start[0]))

    try:
        areas_by_id = areas.find_one({"area_id": id_area})
        if areas_by_id is None:
            return Success("not found")
        accidents_by_area = areas_by_id["accidents"]
        week_accident = weekly.find({"start": date_start})
        if week_accident is None:
            return Success("not found")
        week_accident = week_accident["accidents"]
        accidents_by_area_weekly = list(set(accidents_by_area) & set(week_accident))
        results = accidents.find({'_id': {'$in': accidents_by_area_weekly}})
        return Success(results)
    except PyMongoError as e:
        return Failure(str(e))


def get_accidents_by_day(id_area: id, date: str):
    try:
        date = '/'.join(date)
        breakpoint()
        accidents_by_area = areas.find_one({"area": id_area})
        if accidents_by_area is None:
            return Success("not found")
        accidents_by_area = accidents_by_area["accidents"]
        daily_accident = daily.find_one({"date": date})
        if daily_accident is None:
            return Success("not found")
        daily_accident = daily_accident["accidents"]
        accidents_by_area_daily = list(set(accidents_by_area) & set(daily_accident))
        results = accidents.find({'_id': {'$in': accidents_by_area_daily}})
        return Success(list(results))
    except PyMongoError as e:
        return Failure(str(e))


def get_accidents_by_month(id_area: int, date: tuple):
    year = date[1]
    month = date[0]
    breakpoint()
    try:
        area_doc = areas.find_one({"area": id_area})
        if area_doc is None:
            return Success(f"No accidents found for area {id_area}")
        accidents_by_area = area_doc["accidents"]
        weekly_accident_doc = monthly.find_one({"year": year, "month": month})
        if weekly_accident_doc is None:
            return Success(f"No weekly accidents found for {month}/{year}")
        monthly_accident = weekly_accident_doc["accidents"]
        accidents_by_area_weekly = list(set(accidents_by_area) & set(monthly_accident))
        results_cursor = accidents.find({'_id': {'$in': accidents_by_area_weekly}})
        results = list(results_cursor)

        return Success(results)

    except PyMongoError as e:
        return Failure(str(e))


def get_accidents_by_cause_and_area(area_id: int):
    try:
        accidents_by_area = list(areas.aggregate([
            {"$match": {"area": area_id}},
            {"$lookup": {
                "from": "accident",
                "localField": "accidents",
                "foreignField": "_id",
                "as": "accidents"
            }},
            {"$unwind": "$accidents"},
            {"$project": {
                "accidents._id": 1,
                "accidents.date": 1,
                "accidents.PRIM_CONTRIBUTORY_CAUSE": 1,
                "_id": 0
            }},
            {"$group": {
                "_id": "$accidents.PRIM_CONTRIBUTORY_CAUSE",
                "total_accidents": {"$sum": 1},
                "accidents": {"$push": "$accidents"}
            }}
        ]))

        return Success(accidents_by_area)
    except PyMongoError as e:
        return Failure(str(e))


def get_accidents_by_injured_and_area(area_id: int):
    try:
        breakpoint()
        accidents_by_area = list(areas.aggregate([
            {"$match": {"area": area_id}},
            {"$lookup": {
                "from": "accident",
                "localField": "accidents",
                "foreignField": "_id",
                "as": "accidents"
            }},
            {"$unwind": "$accidents"},
            {"$group": {
                "_id": "$accidents.injuries.total",
                "total_accidents": {"$sum": 1},
                "accidents": {"$push": "$accidents"}
            }
            }
        ]))
        return Success(accidents_by_area)
    except PyMongoError as e:
        return Failure(str(e))
